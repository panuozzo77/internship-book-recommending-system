# core/argument_dispatcher.py
import argparse
import os

from etl.loader import run_etl, exec_all_etl
from utils.logger import LoggerManager  # Your LoggerManager
from core.path_registry import PathRegistry  # Your PathRegistry
from typing import Dict, Any

# Import your ETL runner function (adjust path if different)
# from ETL.simple_etl_processor import run_simple_etl

logger_manager = LoggerManager()


class ArgumentDispatcher:
    def __init__(self, parsed_args: argparse.Namespace, app_config: Dict[str, Any], registry: PathRegistry):
        """
        Initializes the ArgumentDispatcher.
        Args:
            parsed_args: The Namespace object from argparse.parse_args().
            app_config: The loaded application configuration.
            registry: The application's PathRegistry instance.
        """
        self.args = parsed_args
        self.app_config = app_config
        self.registry = registry

    def dispatch(self) -> None:
        """
        Dispatches actions based on the parsed command-line arguments.
        """
        logger = logger_manager.get_logger()
        action_taken = False

        if self.args.load_etl:
            logger.info("Dispatching: Load all configured ETLs action from CLI.")
            self._dispatch_load_all_configured_etls()
            action_taken = True

        if self.args.specific_etl:
            #self._dispatch_load_specific_etl(self.args.specific_etl)
            action_taken = True

        # Add other actions here
        # if self.args.shrink_dataset:
        #     self._dispatch_shrink_dataset(self.args.shrink_dataset)
        #     action_taken = True

        if not action_taken:
            logger.info("No specific CLI action to dispatch (or action not yet implemented in dispatcher).")

    def _dispatch_load_all_configured_etls(self) -> None:
        logger = logger_manager.get_logger()
        """Handles the --load_etl flag for all configured ETLs."""
        logger.info("Dispatching: Loading all configured ETLs action from config file.")

        etl_list = self.app_config.get("etl_list", [])
        etl_configs_base_dir = self.registry.get_path('etl_configs_dir')

        if not etl_list:
            logger.warning("No ETL mapping files listed in 'etl_list' in the application config.")
            return
        if not etl_configs_base_dir:
            logger.error("Path for 'etl_configs_dir' not found in PathRegistry. Cannot locate ETL files.")
            return

        logger.info(f"Found {len(etl_list)} ETL configurations to process: {etl_list}")

        # retrieve the ETL lis files absolute path
        etl_list_paths = [os.path.join(etl_configs_base_dir, etl_file) for etl_file in etl_list]
        exec_all_etl(etl_list_paths, self.app_config, PathRegistry())


    def _run_single_etl_file(self, etl_name_or_path: str, base_dir_for_relative: str, etl_runner_func,
                             json_module) -> None:
        """Helper to run a single ETL file, resolving its path."""
        logger = logger_manager.get_logger()
        etl_mapping_path: str
        if os.path.isabs(etl_name_or_path):
            etl_mapping_path = etl_name_or_path
        else:
            etl_mapping_path = os.path.join(base_dir_for_relative, etl_name_or_path)

        if os.path.exists(etl_mapping_path):
            logger.info(f"Executing ETL with mapping config: {etl_mapping_path}")
            try:
                # The simple_etl_processor.run_simple_etl already loads the ETL JSON itself.
                # We pass the path to it.
                etl_runner_func(etl_mapping_path, self.app_config, self.registry)  # Pass app_config & registry
                logger.info(f"Finished processing ETL from: {etl_mapping_path}")
            except Exception as e:  # Catch broad exceptions from the ETL run itself
                logger.critical(f"ETL pipeline failed for '{etl_mapping_path}'. Error: {e}", exc_info=True)
        else:
            logger.error(f"ETL mapping configuration file not found: {etl_mapping_path}")