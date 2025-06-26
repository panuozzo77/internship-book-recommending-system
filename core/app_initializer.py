# core/app_initializer.py
import os
import argparse  # Still needed for the preliminary --config parse
from utils.logger import LoggerManager
from core.PathRegistry import PathRegistry
from core.app_config_loader import (  # Import new config loading functions
    load_or_create_app_config,
    get_app_config,
    determine_app_config_path,
    DEFAULT_CONFIG_FILENAME  # If needed
)
from .argument_definer import ArgumentDefiner
from .argument_dispatcher import ArgumentDispatcher

# MongoDBConnection will be initialized by the ETL process or other services as needed

logger_manager = LoggerManager()  # Get the singleton instance of LoggerManager


def _setup_paths_from_config(registry: PathRegistry, app_cfg: dict, project_root: str) -> None:
    """Registers paths from app_cfg into PathRegistry and creates directories."""
    if app_cfg and "data_paths" in app_cfg:
        logger = logger_manager.get_logger()
        logger.debug("Registering configured data paths...")
        for alias, rel_path in app_cfg["data_paths"].items():
            full_path = os.path.join(project_root, rel_path)
            registry.set_path(alias, full_path)
            if alias.endswith("_dir") and not os.path.exists(full_path):
                try:
                    os.makedirs(full_path, exist_ok=True)
                    logger.info(f"Created directory: {full_path}")
                except OSError as e:
                    logger.error(f"Could not create directory {full_path}: {e}")
        logger.debug(f"PathRegistry contents: {registry.all_paths()}")
    else:
        logger_manager.get_logger().warning("No 'data_paths' section found in app_config or app_config is empty.")


def initialize_app(registry: PathRegistry) -> None:
    """
    Initializes the application:
    1. Sets up initial logging (done by LoggerManager instantiation).
    2. Ensures project root is defined in PathRegistry (done by run.py).
    3. Parses preliminary CLI for --config to find main config file.
    4. Loads main application configuration (config.json), creates default if needed.
    5. Configures logger fully based on loaded app config.
    6. Registers data paths from app config into PathRegistry.
    7. Defines all CLI arguments.
    8. Parses all CLI arguments.
    9. Dispatches actions based on parsed arguments.
    """
    logger = logger_manager.get_logger()  # Get the pre-init logger
    logger.info("Core application initialization started.")

    project_root = registry.get_path('root')
    if not project_root:
        # This should have been caught by run.py, but as a safeguard
        logger.critical("Project root path not available in PathRegistry during core initialization.")
        # Attempt to guess, highly discouraged here
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        registry.set_path('root', project_root)
        logger.warning(f"Project root re-guessed to: {project_root}")

    # --- 1. Preliminary Parse for --config to determine which main config file to load ---
    # This allows overriding the default config.json location via CLI
    temp_arg_parser = argparse.ArgumentParser(add_help=False)  # Suppress help for this preliminary parse
    temp_arg_parser.add_argument("--config", type=str, metavar="PATH")
    # Parse only known args to avoid errors if other args are present (they'll be parsed later)
    prelim_args, _ = temp_arg_parser.parse_known_args()

    # --- 2. Load Main Application Configuration ---
    app_config_file_path = determine_app_config_path(project_root, prelim_args.config)
    registry.set_path('config_file', app_config_file_path)  # Register the config file path
    if not load_or_create_app_config(app_config_file_path, project_root, bool(prelim_args.config)):
        logger.critical("Application configuration (config.json) could not be established. Initialization halted.")
        # LoggerManager already has a pre-init logger, so it will continue to work.
        return  # Stop further initialization

    current_app_config = get_app_config()  # Get the loaded config

    # --- 3. Configure Logger fully based on loaded App Config ---
    log_settings = current_app_config.get("logging", {})
    logger_manager.setup_logger(
        name=log_settings.get("name", "AppLogger"),
        level=log_settings.get("level", "INFO"),
        log_file=os.path.join(project_root, log_settings.get("log_file", "app.log")) if log_settings.get(
            "log_file") else None
    )
    logger = logger_manager.get_logger()  # Get the newly configured logger
    logger.info("Logger fully configured from application settings.")

    # --- 4. Register Data Paths from App Config ---
    _setup_paths_from_config(registry, current_app_config, project_root)

    # --- 5. Initialize MongoDBConnection Singleton (it will use PathRegistry to find config if needed) ---
    # The MongoDBConnection singleton will load its config when first instantiated.
    # We can trigger its instantiation here if we want to ensure DB connection is attempted early,
    # or let services (like ETL) instantiate it when they need it.
    # For now, let the ETL process initialize it. If you add --ping_db arg, you could init here.
    # from core.mongodb_connection import MongoDBConnection
    # try:
    #     mongo_conn = MongoDBConnection(registry.get_path('config_file')) # Assumes config_file is 'config.json'
    #     # mongo_conn.get_database() # This would attempt connection
    #     logger.info("MongoDB connection manager initialized (connection will be lazy).")
    # except Exception as e:
    #     logger.error(f"Failed to initialize MongoDB connection manager: {e}")

    # --- 6. Define and Parse All CLI Arguments ---
    arg_definer = ArgumentDefiner(current_app_config)
    parser = arg_definer.get_parser()
    # Now parse all arguments using the fully defined parser
    # We use None here so argparse uses sys.argv[1:] by default
    # If prelim_args had other args, they'd be re-parsed here correctly.
    all_cli_args = parser.parse_args()
    logger.debug(f"All CLI arguments parsed: {all_cli_args}")

    # --- 7. Dispatch Actions ---
    logger.info("Dispatching actions based on parsed CLI arguments...")
    logger.info(f"args: {all_cli_args} | app_config: {current_app_config} | registry: {registry.all_paths()}")
    dispatcher = ArgumentDispatcher(all_cli_args, current_app_config, registry)
    dispatcher.dispatch()

    logger.info(
        f"Application '{current_app_config.get('project_name', 'N/A')}' version '{current_app_config.get('version', 'N/A')}' initialization sequence complete.")