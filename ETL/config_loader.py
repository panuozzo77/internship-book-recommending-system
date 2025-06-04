# ETL/config_loader.py
import json
import logging
import os

# Get a logger for this module
logger = logging.getLogger(__name__)


def load_etl_config(config_path: str):
    """
    Loads and validates the ETL-specific mapping/configuration JSON file.

    Args:
        config_path (str): The absolute path to the ETL configuration JSON file.

    Returns:
        dict: The loaded ETL configuration, or None if loading fails.
    """
    logger.info(f"Attempting to load ETL mapping configuration from: {config_path}")
    if not os.path.exists(config_path):
        logger.error(f"ETL mapping configuration file not found: {config_path}")
        return None
    if not os.path.isfile(config_path):
        logger.error(f"ETL mapping configuration path is not a file: {config_path}")
        return None

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            etl_config_data = json.load(f)

        # Basic validation (can be expanded with jsonschema for more robustness)
        if not isinstance(etl_config_data, dict):
            logger.error(f"ETL mapping configuration in {config_path} is not a valid JSON object (dictionary).")
            return None
        if "sources" not in etl_config_data or not isinstance(etl_config_data["sources"], list):
            logger.warning(
                f"ETL mapping configuration in {config_path} is missing a 'sources' list or it's not a list.")
            # Allow proceeding if other parts like 'targets' are present and valid
        if "targets" not in etl_config_data or not isinstance(etl_config_data["targets"], list):
            logger.error(f"ETL mapping configuration in {config_path} is missing a 'targets' list or it's not a list.")
            return None  # Targets are essential

        logger.info(f"Successfully loaded ETL mapping configuration from {config_path}")
        return etl_config_data
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding ETL mapping JSON from {config_path}: {e}")
    except IOError as e:
        logger.error(f"IOError reading ETL mapping configuration file {config_path}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading ETL mapping config {config_path}: {e}", exc_info=True)
    return None


if __name__ == '__main__':
    # Example usage (for testing this module directly)
    logging.basicConfig(level=logging.DEBUG)
    # Create a dummy etl_config_test.json for this test
    dummy_etl_config = {
        "global_settings": {"sample_n_rows": 10},
        "sources": [{"alias": "test_source", "path": "dummy.csv", "format": "csv"}],
        "targets": [
            {"collection_name": "test_collection", "source_dataframe_alias": "test_source", "document_structure": []}]
    }
    test_config_path = "etl_config_test.json"
    with open(test_config_path, "w") as f:
        json.dump(dummy_etl_config, f, indent=2)

    loaded_config = load_etl_config(test_config_path)
    if loaded_config:
        print("Loaded ETL Config:", json.dumps(loaded_config, indent=2))
    else:
        print("Failed to load ETL config.")
    os.remove(test_config_path)  # Clean up