# core/argument_dispatcher.py

import argparse
from typing import Dict, Any

from core.utils.LoggerManager import LoggerManager
from core.PathRegistry import PathRegistry

# Import the entire module to keep the namespace clean
from core import dispatcher_actions

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
        self.logger = logger_manager.get_logger()

    def dispatch(self) -> None:
        """Routes the command to the appropriate action handler."""
        try:
            if not self.args.command:
                self.logger.info("No command specified. Use --help to see available commands.")
                return

            handler = getattr(self, f"_handle_{self.args.command}", None)
            if handler:
                handler()
            else:
                self.logger.error(f"No handler found for command: {self.args.command}")
        except Exception as e:
            self.logger.critical(f"Error dispatching command: {e}", exc_info=True)

    def _handle_etl(self) -> None:
        """Handles ETL subcommand actions."""
        if self.args.load_all:
            dispatcher_actions.load_all_configured_etls(self.app_config, self.registry)
        elif self.args.specific:
            dispatcher_actions.load_specific_etl(self.args.specific, self.app_config, self.registry)

    def _handle_recommend(self) -> None:
        """Handles recommendation subcommand actions."""
        if self.args.by_title:
            dispatcher_actions.recommend_by_titles(self.args.by_title, self.args.top_n)
        elif self.args.by_user_id:
            dispatcher_actions.recommend_for_user_id(self.args.by_user_id, self.args.top_n)
        elif self.args.by_profile_file:
            dispatcher_actions.recommend_from_profile_file(self.args.by_profile_file, self.args.top_n)

    def _handle_tools(self) -> None:
        """Handles data tools subcommand actions."""
        if self.args.infer_schema:
            dispatcher_actions.infer_schema(
                input_dir=self.args.schema_input_dir,
                output_path=self.args.schema_output_path,
                output_mode=self.args.schema_output_mode
            )

    def _handle_webui(self) -> None:
        """Handles web UI subcommand action."""
        dispatcher_actions.run_web_ui(self.app_config)