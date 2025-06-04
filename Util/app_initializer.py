# Util/Main/app_initializer.py

import os
import json
import argparse
import logging
from Util.PathRegistry import PathRegistry  # Assuming PathRegistry is in Util/PathRegistry.py

DEFAULT_CONFIG_FILENAME = "config.json"
DEFAULT_ETL_CONFIG_FILENAME = "default_etl_mapping.json"  # For later use

# Global application configuration dictionary
APP_CONFIG = {}


def create_default_config(filepath):
    """Creates a default configuration file at the specified path."""
    default_config_data = {
        "project_name": "GoodreadsRecommender",
        "version": "0.1.0",
        "author": "Your Name/Group",
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(levelname)s - %(module)s - %(message)s"
        },
        "database": {
            "type": "mongodb",
            "uri": "mongodb://localhost:27017/",
            "db_name": "goodreads_project_db",
            "default_books_collection": "books",
            "default_authors_collection": "authors",
            "default_users_collection": "users",
            "default_interactions_collection": "interactions"
        },
        "data_paths": {
            # Relative to project root, PathRegistry can resolve these
            "raw_datasets_dir": "downloaded_datasets/partial/",
            "processed_datasets_dir": "processed_data/",
            "etl_configs_dir": "etl_configurations/"
        },
        "etl_settings": {
            "default_etl_config": DEFAULT_ETL_CONFIG_FILENAME  # Name of the default ETL mapping file
        }
    }
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(default_config_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Default configuration file created at: {filepath}")
        return default_config_data
    except IOError as e:
        logging.error(f"Error creating default configuration file at {filepath}: {e}")
        return None


def load_app_config(filepath):
    """Loads the application configuration from the specified JSON file."""
    global APP_CONFIG
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            APP_CONFIG = json.load(f)
        logging.info(f"Application configuration loaded from: {filepath}")
        return True
    except FileNotFoundError:
        logging.warning(f"Configuration file not found: {filepath}. Attempting to create a default.")
        return False
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding configuration JSON from {filepath}: {e}")
        APP_CONFIG = {}  # Reset to empty if load fails
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading configuration from {filepath}: {e}")
        APP_CONFIG = {}
        return False


def setup_logging(config=None):
    """Configures logging based on the loaded configuration."""
    if config is None:  # Fallback if APP_CONFIG is not yet populated or logging config missing
        log_config = {"level": "INFO", "format": "%(asctime)s - %(levelname)s - %(module)s - %(message)s"}
    else:
        log_config = config.get("logging",
                                {"level": "INFO", "format": "%(asctime)s - %(levelname)s - %(module)s - %(message)s"})

    log_level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_format = log_config.get("format", "%(asctime)s - %(levelname)s - %(module)s - %(message)s")

    # Remove any existing handlers to avoid duplicate logs if called multiple times
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=log_level, format=log_format)
    logging.info("Logging configured.")


def initialize_app():
    global APP_CONFIG
    registry = PathRegistry()

    # 0. Basic logging setup first
    setup_logging() # Call without APP_CONFIG first for early messages

    project_root = registry.get_path('root')
    if not project_root:
        logging.critical("Project root path not set in PathRegistry. This should be set by the calling script (e.g., run.py).")
        # Attempt to guess, but this is risky
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Assumes Util is one level down
        registry.set_path('root', project_root)
        logging.warning(f"Project root was not set, auto-guessed to: {project_root}")

    config_filepath_cli = None # For --custom_config later

    # Argument parsing (basic for now)
    parser = argparse.ArgumentParser(description="Goodreads Recommender Application")
    parser.add_argument("--config", type=str, help="Path to a custom application configuration file (config.json).")
    parser.add_argument("--load_etl", type=str, metavar="ETL_CONFIG_NAME",
                        help="Name of the ETL mapping JSON file (e.g., my_etl.json) located in etl_configurations_dir, or an absolute path to an ETL config file.")
    # Add more arguments here
    args = parser.parse_args()

    if args.config:
        logging.info(f"CLI argument --config provided: {args.config}")
        config_filepath_cli = args.config # This could be absolute or relative to CWD

    # Determine primary config file path
    if config_filepath_cli:
        if os.path.isabs(config_filepath_cli):
            primary_config_filepath = config_filepath_cli
        else:
            primary_config_filepath = os.path.join(os.getcwd(), config_filepath_cli) # Relative to current working dir
    else:
        primary_config_filepath = os.path.join(project_root, DEFAULT_CONFIG_FILENAME)

    logging.info(f"Attempting to load primary application configuration from: {primary_config_filepath}")

    # Load configuration or create default
    if not load_app_config(primary_config_filepath): # Tries to load
        if not config_filepath_cli: # Only create default if no custom config was specified
            default_path_to_create = os.path.join(project_root, DEFAULT_CONFIG_FILENAME)
            logging.info(f"Primary config not found at '{primary_config_filepath}'. Creating default at '{default_path_to_create}'.")
            if create_default_config(default_path_to_create):
                load_app_config(default_path_to_create) # Load the newly created default
            else:
                logging.critical("Failed to load or create a default configuration file. Application may not function correctly.")
                setup_logging() # Ensure basic logging if all fails
                return # Exit initialization if config is essential
        else:
            logging.critical(f"Specified custom config '{primary_config_filepath}' not found. Cannot proceed.")
            setup_logging()
            return


    # Re-configure logging based on loaded configuration
    setup_logging(APP_CONFIG)

    # Register other important paths from config into PathRegistry
    if APP_CONFIG and "data_paths" in APP_CONFIG:
        for alias, rel_path in APP_CONFIG["data_paths"].items():
            full_path = os.path.join(project_root, rel_path)
            registry.set_path(alias, full_path)
            if alias.endswith("_dir") and not os.path.exists(full_path):
                try:
                    os.makedirs(full_path, exist_ok=True)
                    logging.info(f"Created directory: {full_path}")
                except OSError as e:
                    logging.error(f"Could not create directory {full_path}: {e}")
        logging.debug(f"Registered data paths: {registry.all_paths()}")

    logging.info(f"Application '{APP_CONFIG.get('project_name', 'N/A')}' version '{APP_CONFIG.get('version', 'N/A')}' initialized.")

    # --- Handle ETL Loading ---
    if args.load_etl:
        etl_config_name_or_path = args.load_etl
        logging.info(f"ETL load requested for configuration: '{etl_config_name_or_path}'")

        # Dynamically import here to ensure sys.path is set up by run.py
        try:
            from ETL.etl_runner import run_etl_pipeline
        except ImportError as e:
            logging.critical(f"Could not import ETL runner. Ensure ETL modules are correctly placed and project root is in PYTHONPATH. Error: {e}")
            return

        etl_config_path_to_run = None
        if os.path.isabs(etl_config_name_or_path):
            etl_config_path_to_run = etl_config_name_or_path
        else:
            # Assume it's a name relative to etl_configs_dir
            etl_configs_base_dir = registry.get_path('etl_configs_dir')
            if not etl_configs_base_dir:
                logging.error("Path for 'etl_configs_dir' not found in PathRegistry. Cannot resolve relative ETL config path.")
                return
            etl_config_path_to_run = os.path.join(etl_configs_base_dir, etl_config_name_or_path)

        if os.path.exists(etl_config_path_to_run):
            logging.info(f"Executing ETL pipeline with mapping config: {etl_config_path_to_run}")
            try:
                run_etl_pipeline(etl_config_path_to_run, APP_CONFIG)
            except Exception as e:
                logging.critical(f"ETL pipeline failed for '{etl_config_path_to_run}'. Error: {e}", exc_info=True)
        else:
            logging.error(f"ETL mapping configuration file not found: {etl_config_path_to_run}")
    else:
        logging.info("No specific ETL action requested via CLI arguments.")

    # Other actions based on CLI arguments can be handled here