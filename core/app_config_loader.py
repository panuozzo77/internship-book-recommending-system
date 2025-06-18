# core/app_config_loader.py
import json
import os
from utils.logger import LoggerManager # Use your LoggerManager
from typing import Optional, Dict, Any

DEFAULT_CONFIG_FILENAME = "config.json"
_APP_CONFIG: Optional[Dict[str, Any]] = None # Module-level global for loaded config

logger = LoggerManager().get_logger()

def load_or_create_app_config(config_filepath: str, project_root_for_default: str, is_custom_path: bool) -> bool:
    """
    Loads the application configuration from config_filepath.
    If it doesn't exist and it's not a custom path, creates a default one.
    Updates the module-level _APP_CONFIG.
    Returns True if config loaded/created successfully, False otherwise.
    """
    global _APP_CONFIG
    logger.info(f"Attempting to load application configuration from: {config_filepath}")

    try:
        with open(config_filepath, 'r', encoding='utf-8') as f:
            _APP_CONFIG = json.load(f)
        logger.info(f"Application configuration loaded successfully from: {config_filepath}")
        return True
    except FileNotFoundError:
        logger.debug(f"Configuration file not found at: {config_filepath}")
        if not is_custom_path:
            # Only create default if no custom config path was specified AND we are checking the default path
            default_path_to_create = os.path.join(project_root_for_default, DEFAULT_CONFIG_FILENAME)
            if config_filepath == default_path_to_create:
                logger.info(f"Primary config not found. Creating default at '{default_path_to_create}'.")
                if _create_default_config_file(default_path_to_create):
                    return load_or_create_app_config(default_path_to_create, project_root_for_default, False) # Reload after creation
                else:
                    logger.critical(f"Failed to create default configuration at {default_path_to_create}.")
            else:
                logger.warning(f"Config not found at '{config_filepath}', but it wasn't the expected default path. Not creating default.")
        else:
            logger.critical(f"Specified custom config '{config_filepath}' not found.")
        return False # Config not successfully loaded or created
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding configuration JSON from {config_filepath}: {e}")
        _APP_CONFIG = {}
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred loading configuration from {config_filepath}: {e}", exc_info=True)
        _APP_CONFIG = {}
        return False

def _create_default_config_file(filepath: str) -> bool:
    """Creates a default configuration file at the specified path."""
    default_config_data = {
        "project_name": "GoodreadsRecommender",
        "version": "0.1.0",
        "author": "Your Name/Group",
        "logging": {
            "name": "AppLogger", # Name for the logger instance
            "level": "INFO",
            "log_file": "app.log" # Relative to project root, or absolute
        },
        "database": {
            "type": "mongodb",
            "uri": "mongodb://localhost:27017/",
            "db_name": "goodreads_recommender_db" # Default DB name
        },
        "data_paths": {
            "raw_datasets_dir": "downloaded_datasets/partial/",
            "processed_datasets_dir": "processed_data/",
            "etl_configs_dir": "etl_configurations/",
            "log_dir": "logs/" # For log files
        },
        "etl_list": ["etl_libri.json"], # Example ETL mapping file
        "webapp": {
            "host": "127.0.0.1",
            "port": 5001,
            "debug": true
        }  
    }
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True) # Ensure directory exists
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(default_config_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Default configuration file created at: {filepath}")
        return True
    except IOError as e:
        logger.error(f"Error creating default configuration file at {filepath}: {e}")
        return False

def get_app_config() -> Optional[Dict[str, Any]]:
    """Returns the loaded application configuration."""
    if _APP_CONFIG is None:
        logger.warning("Application config accessed before it was loaded.")
    return _APP_CONFIG

def determine_app_config_path(project_root: str, cli_custom_config_path: Optional[str] = None) -> str:
    """Determines the path to the application's main configuration file."""
    if cli_custom_config_path:
        logger.debug(f"Custom application config path provided via CLI: {cli_custom_config_path}")
        if os.path.isabs(cli_custom_config_path):
            return cli_custom_config_path
        else:
            return os.path.join(os.getcwd(), cli_custom_config_path) # Relative to CWD
    else:
        return os.path.join(project_root, DEFAULT_CONFIG_FILENAME)