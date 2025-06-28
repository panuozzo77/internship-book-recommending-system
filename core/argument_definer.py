# core/argument_definer.py
import argparse
import sys
from typing import Optional, Dict, Any


class ArgumentDefiner:
    def __init__(self, app_config: Optional[Dict[str, Any]] = None):
        """
        Initializes the ArgumentDefiner with optional application configuration.
        
        Args:
            app_config: Dictionary containing application configuration that might be used
                       for argument descriptions and defaults.
        """
        self.app_config = app_config or {}
        self.parser = self._create_parser()

    def _create_parser(self) -> argparse.ArgumentParser:
        """Creates and configures the root ArgumentParser with subcommands."""
        project_name = self.app_config.get("project_name", "Application")
        
        parser = argparse.ArgumentParser(
            description=f"{project_name} - Command Line Interface",
            epilog="Usage examples:\n"
                   "  python run.py etl --load-all\n"
                   "  python run.py recommend --by-title 'Dune' 'The Hobbit'\n"
                   "  python run.py recommend --by-user-id '8842281e1d1347389f2ab93d60773d4d' --top-n 5\n"
                   "  python run.py webui\n"
                   "  python run.py tools --infer-schema --schema-input-dir ./data",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        # Global arguments that apply to all commands
        parser.add_argument(
            "--config",
            type=str,
            metavar="PATH",
            help="Path to a custom application configuration file (overrides default)"
        )

        # Subcommands
        subparsers = parser.add_subparsers(
            dest="command",
            title="Available Commands",
            description="Choose one of the following commands to proceed. Use 'command --help' for details.",
            required=True
        )

        self._add_etl_subparser(subparsers)
        self._add_recommend_subparser(subparsers)
        self._add_tools_subparser(subparsers)
        self._add_webui_subparser(subparsers)

        return parser

    def _add_etl_subparser(self, subparsers: argparse._SubParsersAction) -> None:
        """Adds the ETL subcommand parser."""
        etl_parser = subparsers.add_parser(
            "etl",
            help="Run Extract, Transform, Load (ETL) processes",
            description="Load data from source files into MongoDB according to configurations."
        )

        # Mutually exclusive ETL actions
        action_group = etl_parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument(
            "--load-all",
            action="store_true",
            help="Run all ETL processes defined in configuration"
        )
        action_group.add_argument(
            "--specific",
            type=str,
            metavar="ETL_CONFIG_NAME",
            help="Run a specific ETL process (e.g., 'etl_books.json')"
        )

    def _add_recommend_subparser(self, subparsers: argparse._SubParsersAction) -> None:
        """Adds the recommendation subcommand parser."""
        rec_parser = subparsers.add_parser(
            "recommend",
            help="Generate book recommendations",
            description="Generate recommendations based on book titles or user profiles."
        )

        # Recommendation methods (mutually exclusive)
        method_group = rec_parser.add_mutually_exclusive_group(required=True)
        method_group.add_argument(
            "--by-title",
            nargs='+',
            type=str,
            metavar="TITLE",
            help="Recommend based on a list of book titles"
        )
        method_group.add_argument(
            "--by-user-id",
            type=str,
            metavar="USER_ID",
            help="Recommend for an existing user from database"
        )
        method_group.add_argument(
            "--by-profile-file",
            type=str,
            metavar="FILE_PATH",
            help="Recommend based on a local profile file"
        )

        # Recommendation modifiers
        rec_parser.add_argument(
            "--top-n",
            type=int,
            default=10,
            help="Number of recommendations to return (default: 10)"
        )

    def _add_tools_subparser(self, subparsers: argparse._SubParsersAction) -> None:
        """Adds the data tools subcommand parser."""
        tools_parser = subparsers.add_parser(
            "tools",
            help="Data analysis and enrichment tools",
            description="Perform operations like schema inference and data augmentation."
        )

        # Tools actions (mutually exclusive)
        action_group = tools_parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument(
            "--infer-schema",
            action="store_true",
            help="Perform schema inference from source files"
        )
        # Can add more tools here as needed

        # Schema inference arguments
        tools_parser.add_argument(
            "--schema-input-dir",
            type=str,
            required='--infer-schema' in sys.argv,
            metavar="PATH",
            help="Input directory containing JSON/JSONL files for schema inference"
        )
        tools_parser.add_argument(
            "--schema-output-path",
            type=str,
            metavar="PATH",
            help="Output file/directory for inferred schemas"
        )
        tools_parser.add_argument(
            "--schema-output-mode",
            choices=['individual', 'aggregate', 'both'],
            default='both',
            help="Schema output mode (default: both)"
        )

    def _add_webui_subparser(self, subparsers: argparse._SubParsersAction) -> None:
        """Adds the Web UI subcommand parser."""
        subparsers.add_parser(
            "webui",
            help="Launch the web user interface",
            description="Start a local web server for book management and ratings."
        )

    def get_parser(self) -> argparse.ArgumentParser:
        """Returns the configured argument parser."""
        return self.parser