# core/argument_definer.py
import argparse
from typing import Optional, Dict, Any

class ArgumentDefiner:
    def __init__(self, app_config: Optional[Dict[str, Any]] = None):
        """
        Initializes the ArgumentDefiner.
        Args:
            app_config: The loaded application configuration (optional, can be used for descriptions).
        """
        self.app_config = app_config if app_config else {}
        self.parser = self._create_parser()

    def _create_parser(self) -> argparse.ArgumentParser:
        """Creates and configures the argparse.ArgumentParser instance."""
        project_name = self.app_config.get("project_name", "Application")
        parser = argparse.ArgumentParser(
            description=f"{project_name} Command Line Interface",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter # Shows defaults in help
        )

        parser.add_argument(
            "--config",
            type=str,
            metavar="PATH",
            help="Path to a custom application configuration file (e.g., config.json). Overrides default."
        )
        parser.add_argument(
            "--load_etl",
            action="store_true",
            help="Run all ETL processes defined in the main application config's 'etl_list'."
        )
        parser.add_argument(
            "--specific_etl",
            type=str,
            metavar="ETL_CONFIG_NAME_OR_PATH",
            help="Run a specific ETL mapping file. Provide filename from 'etl_configurations_dir' or an absolute path."
        )
        parser.add_argument(
            "--recommend",
            nargs='+', # Accetta uno o più valori (i titoli dei libri)
            type=str,
            metavar="BOOK_TITLE",
            help="Get book recommendations based on a list of input book titles."
        )
        parser.add_argument(
            "--top_n",
            type=int,
            default=10,
            help="Number of recommendations to return. Used with --recommend."
        )
        parser.add_argument(
            "--user_profile",
            type=str,
            metavar="USER_ID",
            help="Get recommendations for a specific user ID from the database."
        )
        parser.add_argument(
            "--webui",
            action="store_true",
            help="Run the web user interface for the application."
        )
        # Add more arguments here if needed (e.g., for shrink_dataset)
        # parser.add_argument(
        #     "--shrink_dataset", ...
        # )
        return parser

    def get_parser(self) -> argparse.ArgumentParser:
        return self.parser