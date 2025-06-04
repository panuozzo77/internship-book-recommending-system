# ETL/etl_runner.py
import logging
from .config_loader import load_etl_config # Relative import
from .data_loader import load_all_sources   # Relative import
from .data_joiner import perform_joins    # Relative import
from .mongodb_mapper import load_to_mongodb # Relative import
from .DatabaseManager import DatabaseManager # Relative import
from Util.PathRegistry import PathRegistry # Assuming Util is accessible from ETL context
                                          # This might require sys.path manipulation if run directly,
                                          # but should be fine when called from run.py

# Logging configuration will be handled by the main app_initializer
# logger = logging.getLogger(__name__) # Use module-specific logger

def run_etl_pipeline(etl_config_path, app_config):
    """
    Runs the full ETL pipeline based on the ETL configuration file.
    Args:
        etl_config_path (str): Absolute path to the ETL mapping/configuration JSON file.
        app_config (dict): The main application configuration (from config.json).
    """
    logging.info(f"Starting ETL pipeline with ETL config: {etl_config_path}")

    # 1. Load ETL Specific Configuration (the mapping config)
    try:
        etl_mapping_config = load_etl_config(etl_config_path)
        if not etl_mapping_config: # Ensure load_etl_config returns None or raises on failure
            logging.critical("ETL mapping configuration could not be loaded. Aborting ETL.")
            return
    except Exception as e:
        logging.critical(f"Failed to load ETL mapping config '{etl_config_path}'. Aborting. Error: {e}")
        return

    # 2. Initialize DatabaseManager using main app_config
    db_settings = app_config.get("database", {})
    db_uri = db_settings.get("uri", "mongodb://localhost:27017/")
    db_name = db_settings.get("db_name", "default_etl_db") # Or use a specific DB for this ETL run

    try:
        # Pass the global_settings from etl_mapping_config to DB manager if it defines its own DB
        # For now, assume ETL uses the app's main DB config.
        db_manager = DatabaseManager(db_uri=db_uri, db_name=db_name)
        if not db_manager.is_connected():
            logging.critical("Failed to connect to MongoDB for ETL. Aborting.")
            return
    except Exception as e:
        logging.critical(f"Failed to initialize DatabaseManager for ETL. Aborting. Error: {e}")
        return

    # Adjust paths in etl_mapping_config to be absolute, based on PathRegistry
    # This is important if paths in etl_mapping_config.sources are relative
    registry = PathRegistry()
    raw_datasets_base_dir = registry.get_path('raw_datasets_dir', '.') # Default to current if not set

    for source in etl_mapping_config.get("sources", []):
        if not os.path.isabs(source['path']):
            source['path'] = os.path.join(raw_datasets_base_dir, source['path'])
            logging.debug(f"Resolved source path for '{source['alias']}' to '{source['path']}'")


    # 3. Load Source Data into DataFrames
    logging.info("--- ETL: Loading Source Data ---")
    # Pass etl_mapping_config which contains 'sources' and 'global_settings' for ETL
    dataframes = load_all_sources(etl_mapping_config)
    if not dataframes:
        logging.warning("ETL: No dataframes were loaded. Check source configurations in ETL mapping and file paths.")
        db_manager.close_connection()
        return

    # 4. Perform Joins
    logging.info("--- ETL: Performing Joins ---")
    if "joins" in etl_mapping_config and etl_mapping_config["joins"]:
        dataframes = perform_joins(dataframes, etl_mapping_config["joins"])
    else:
        logging.info("ETL: No join operations defined in the ETL mapping config.")

    # 5. Map and Load to MongoDB
    logging.info("--- ETL: Loading to MongoDB ---")
    if "targets" in etl_mapping_config and etl_mapping_config["targets"]:
        load_to_mongodb(dataframes, etl_mapping_config["targets"], db_manager)
    else:
        logging.warning("ETL: No target MongoDB collections defined in the ETL mapping config.")

    # 6. Clean up
    db_manager.close_connection()
    logging.info(f"ETL pipeline finished for config: {etl_config_path}")