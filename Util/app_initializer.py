# Util/app_initializer.py
import argparse
import os
import json
# import argparse # No longer needed here directly for parsing, ArgHandler does it
import logging
import sys
from .PathRegistry import PathRegistry
from .ArgHandler import ArgHandler # Import the new handler

DEFAULT_CONFIG_FILENAME = "config.json"
APP_CONFIG = {}

# --- Helper Functions for Initialization (largely the same as before) ---
def _setup_initial_logging(): # No change
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - PRE-CONFIG - %(module)s - %(message)s",
                        force=True)
    logging.info("Initial basic logging configured.")

def _ensure_project_root(registry: PathRegistry): # No change
    project_root = registry.get_path('root')
    if not project_root:
        logging.critical("Project root path not set in PathRegistry.")
        guessed_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        registry.set_path('root', guessed_project_root)
        logging.warning(f"Project root was not set by caller, auto-guessed to: {guessed_project_root}")
        return guessed_project_root
    return project_root

# _parse_cli_arguments is now handled by ArgHandler

def _determine_app_config_path(project_root: str, cli_custom_config_arg: str = None): # Modified to take arg
    """Determines the path to the application's main configuration file."""
    if cli_custom_config_arg: # Check if the argument from ArgHandler.args.config is present
        logging.info(f"Custom application config path provided via CLI: {cli_custom_config_arg}")
        if os.path.isabs(cli_custom_config_arg):
            return cli_custom_config_arg
        else:
            return os.path.join(os.getcwd(), cli_custom_config_arg)
    else:
        return os.path.join(project_root, DEFAULT_CONFIG_FILENAME)

def load_app_config_from_file(filepath: str): # No change
    global APP_CONFIG
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            APP_CONFIG = json.load(f)
        logging.info(f"Application configuration loaded successfully from: {filepath}")
        return True
    except FileNotFoundError:
        logging.debug(f"Configuration file not found at: {filepath}")
        return False
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding configuration JSON from {filepath}: {e}")
        APP_CONFIG = {}
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading configuration from {filepath}: {e}")
        APP_CONFIG = {}
        return False

def create_default_config_file(filepath: str): # No change
    default_config_data = {
        "project_name": "GoodreadsRecommender",
        "version": "0.1.0",
        "author": "Your Name/Group",
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s"
        },
        "database": {
            "type": "mongodb",
            "uri": "mongodb://localhost:27017/",
            "db_name": "goodreads_project_db"
        },
        "data_paths": {
            "raw_datasets_dir": "downloaded_datasets/partial/",
            "processed_datasets_dir": "processed_data/",
            "etl_configs_dir": "etl_configurations/"
        },
        "etl_settings": {
            "default_etl_config_name": "example_book_load_config.json"
        }
    }
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(default_config_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Default configuration file created at: {filepath}")
        return True
    except IOError as e:
        logging.error(f"Error creating default configuration file at {filepath}: {e}")
        return False

def _load_or_create_app_config(config_filepath: str, project_root_for_default: str, is_custom_path_from_cli: bool): # No change
    global APP_CONFIG
    logging.info(f"Attempting to load application configuration from: {config_filepath}")
    if load_app_config_from_file(config_filepath):
        return True
    else:
        if not is_custom_path_from_cli:
            default_path_to_create = os.path.join(project_root_for_default, DEFAULT_CONFIG_FILENAME)
            if config_filepath == default_path_to_create: # Ensure we are trying to create the default
                 logging.info(f"Primary config not found at '{config_filepath}'. Creating default.")
                 if create_default_config_file(default_path_to_create):
                     return load_app_config_from_file(default_path_to_create)
                 else:
                     logging.critical(f"Failed to create a default configuration file at {default_path_to_create}.")
            else:
                logging.warning(f"Config not found at '{config_filepath}', and it's not the expected default path. Not creating default here.")
        else:
            logging.critical(f"Specified custom config '{config_filepath}' not found or failed to load.")
        return False

def _reconfigure_logging_from_app_config(): # No change
    if not APP_CONFIG:
        logging.warning("APP_CONFIG is empty. Using default logging settings.")
        log_settings = None
    else:
        log_settings = APP_CONFIG.get("logging")

    if log_settings:
        level_str = log_settings.get("level", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)
        fmt = log_settings.get("format", "%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s")
    else:
        level = logging.INFO
        fmt = "%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s"

    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.basicConfig(level=level, format=fmt, force=True)
    logging.info(f"Logging reconfigured to level {logging.getLevelName(level)} from application config.")

def _register_configured_paths(registry: PathRegistry, project_root: str): # No change
    if APP_CONFIG and "data_paths" in APP_CONFIG:
        logging.debug("Registering configured data paths...")
        for alias, rel_path in APP_CONFIG["data_paths"].items():
            full_path = os.path.join(project_root, rel_path)
            registry.set_path(alias, full_path)
            if alias.endswith("_dir") and not os.path.exists(full_path):
                try:
                    os.makedirs(full_path, exist_ok=True)
                    logging.info(f"Created directory: {full_path}")
                except OSError as e:
                    logging.error(f"Could not create directory {full_path}: {e}")
        logging.debug(f"PathRegistry contents: {registry.all_paths()}")
    else:
        logging.warning("No 'data_paths' section found in APP_CONFIG or APP_CONFIG is empty.")

# _handle_etl_load_action is now part of ArgHandler.py

# --- Main Initialization Function ---
def initialize_app():
    """
    Initializes the application:
    1. Sets up initial logging.
    2. Ensures project root is defined.
    3. Instantiates ArgHandler which defines CLI arguments.
    4. ArgHandler parses CLI arguments.
    5. Determines and loads the main application configuration (config.json), creating a default if necessary.
    6. Reconfigures logging based on loaded app config.
    7. Registers data paths from app config.
    8. ArgHandler dispatches actions based on parsed CLI arguments.
    """
    _setup_initial_logging()

    registry = PathRegistry()
    project_root = _ensure_project_root(registry)

    # First, parse args to see if a custom config path is provided
    # We need a temporary ArgHandler instance or a static method to parse args
    # before APP_CONFIG is fully loaded, if its parser depends on APP_CONFIG.
    # For simplicity, let's assume _create_parser in ArgHandler doesn't strictly need APP_CONFIG yet for its basic structure.
    # Or, we can parse only the --config argument first.
    # Let's try to load a minimal APP_CONFIG for ArgHandler or pass None

    # Step 1: Parse CLI arguments to know if a custom config.json is specified
    # The ArgHandler will be fully initialized after APP_CONFIG is loaded.
    # For now, we only need the value of args.config from a preliminary parse.
    prelim_parser = argparse.ArgumentParser(add_help=False) # Temporary parser for --config only
    prelim_parser.add_argument("--config", type=str)
    cli_args_known, _ = prelim_parser.parse_known_args() # Parse only known args, ignore others for now

    app_config_path = _determine_app_config_path(project_root, cli_args_known.config)

    if not _load_or_create_app_config(app_config_path, project_root, bool(cli_args_known.config)):
        logging.critical("Application configuration could not be established. Initialization aborted.")
        return # Basic logging is set, exit

    _reconfigure_logging_from_app_config() # Now use the loaded APP_CONFIG

    # Now that APP_CONFIG is loaded, fully initialize ArgHandler
    arg_handler = ArgHandler(APP_CONFIG, registry)
    args = arg_handler.parse_arguments() # Parse all defined arguments

    # Re-check if args.config was provided, as it influences APP_CONFIG source.
    # The logic for _determine_app_config_path and _load_or_create_app_config already handled this.
    # If args.config led to a different app_config_path that was successfully loaded,
    # APP_CONFIG is now from that custom file.

    _register_configured_paths(registry, project_root)

    logging.info(f"Application '{APP_CONFIG.get('project_name', 'N/A')}' version '{APP_CONFIG.get('version', 'N/A')}' fully initialized.")

    # Dispatch actions based on parsed arguments
    arg_handler.handle_actions()

    logging.debug("initialize_app finished.")