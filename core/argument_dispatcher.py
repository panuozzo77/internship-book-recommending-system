# core/argument_dispatcher.py

import argparse
from typing import Dict, Any

from utils.logger import LoggerManager
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

    def dispatch(self) -> None:
        """
        Dispatches actions based on the parsed command-line arguments.
        """
        logger = logger_manager.get_logger()
        action_taken = False

        if self.args.load_etl:
            # CHANGED: Call the function from the actions module and pass the required context.
            dispatcher_actions.load_all_configured_etls(self.app_config, self.registry)
            action_taken = True

        # if self.args.specific_etl:
        #     dispatcher_actions.dispatch_load_specific_etl(self.args.specific_etl, self.app_config, self.registry)
        #     action_taken = True

        if self.args.recommend:
            # CHANGED: Call the function and pass the parsed arguments.
            dispatcher_actions.get_recommendations(self.args)
            action_taken = True
        
        #if self.args.profile_file:
        #    # CHANGED: Call the function for handling profile files.
        #    dispatcher_actions.recommend_from_profile_file(self.args)
        #    action_taken = True
        
        if self.args.user_profile:
            # CHANGED: Call the function for handling user IDs.
            dispatcher_actions.recommend_for_user_id(self.args)
            action_taken = True
        
        if self.args.webui:
            # CHANGED: Call the function and pass the application config.
            dispatcher_actions.run_web_ui(self.app_config)
            action_taken = True

        if not action_taken:
            logger.info("No specific CLI action was taken. Use --help to see available commands.")