# Util/ArgHandler.py
import argparse
import logging
import os


# We'll need PathRegistry and APP_CONFIG from app_initializer, so we'll pass them or import carefully.
# For now, let's assume APP_CONFIG and PathRegistry are accessible or passed.

class ArgHandler:
    def __init__(self, app_config, path_registry):
        """
        Initializes the argument handler.
        Args:
            app_config (dict): The loaded application configuration.
            path_registry (PathRegistry): The application's path registry.
        """
        self.app_config = app_config
        self.registry = path_registry
        self.parser = self._create_parser()
        self.args = None  # Parsed arguments will be stored here

    def _create_parser(self):
        """Creates and configures the argparse.ArgumentParser instance."""
        project_name = self.app_config.get("project_name", "Application")
        parser = argparse.ArgumentParser(description=f"{project_name} Command Line Interface")

        parser.add_argument(
            "--config",
            type=str,
            help="Path to a custom application configuration file (e.g., config.json). Overrides default."
        )
        parser.add_argument(
            "--load_etl",
            type=str,
            metavar="ETL_CONFIG_NAME_OR_PATH",
            help="Name of the ETL mapping JSON file (e.g., my_etl.json) from etl_configurations_dir, "
                 "or an absolute path to an ETL config file to execute."
        )
        parser.add_argument(
            "--shrink_dataset",
            type=str,
            metavar="INPUT_PATH;OUTPUT_PATH;NUM_LINES",
            help="Shrinks a dataset. Provide input_path, output_path, and num_lines separated by semicolons. "
                 "Example: 'data/large.json;data/small.json;100'"
        )
        # TODO: Add more arguments as your application grows
        # parser.add_argument("--run_recommender", action="store_true", help="Runs the recommendation system.")
        # parser.add_argument("--augment_data", action="store_true", help="Starts the data augmentation process.")
        # parser.add_argument("--verbosity", "-v", action="count", default=0, help="Increase output verbosity.")

        return parser

    def parse_arguments(self, custom_args_list=None):
        """
        Parses command-line arguments.
        Args:
            custom_args_list (list, optional): A list of strings to parse, for testing.
                                              If None, sys.argv[1:] is used.
        Returns:
            argparse.Namespace: The parsed arguments.
        """
        self.args = self.parser.parse_args(args=custom_args_list)
        logging.debug(f"CLI arguments parsed: {self.args}")
        return self.args

    def handle_actions(self):
        """
        Dispatches actions based on the parsed command-line arguments.
        This function will call the appropriate handlers/modules.
        """
        if not self.args:
            logging.warning("Arguments not parsed yet. Call parse_arguments() first.")
            return

        # --- ETL Loading Action ---
        if self.args.load_etl:
            self._handle_etl_load_action(self.args.load_etl)

        # --- Shrink Dataset Action ---
        if self.args.shrink_dataset:
            self._handle_shrink_dataset_action(self.args.shrink_dataset)

        # --- Other Actions ---
        # if self.args.run_recommender:
        #     self._handle_run_recommender_action()

        # if self.args.augment_data:
        #     self._handle_augment_data_action()

        if not (self.args.load_etl or self.args.shrink_dataset):  # Add other actions here
            logging.info(
                "No specific action requested via CLI arguments.")

    def _handle_etl_load_action(self, etl_config_name_or_path):
        """Handles the --load_etl command-line action."""
        logging.info(f"ETL load action initiated for: '{etl_config_name_or_path}'")
        try:
            # Import here to avoid circular dependencies if ETL modules also use parts of Util
            from ETL.etl_runner import run_etl_pipeline
        except ImportError as e:
            logging.critical(f"Could not import ETL runner. Ensure ETL modules are correctly placed. Error: {e}")
            return

        etl_config_path_to_run = None
        if os.path.isabs(etl_config_name_or_path):
            etl_config_path_to_run = etl_config_name_or_path
        else:
            etl_configs_base_dir = self.registry.get_path('etl_configs_dir')
            if not etl_configs_base_dir:
                logging.error(
                    "Path for 'etl_configs_dir' not found in PathRegistry. Cannot resolve relative ETL config path.")
                return
            etl_config_path_to_run = os.path.join(etl_configs_base_dir, etl_config_name_or_path)

        if os.path.exists(etl_config_path_to_run):
            logging.info(f"Executing ETL pipeline with mapping config: {etl_config_path_to_run}")
            try:
                # Pass the main application config to the ETL runner
                run_etl_pipeline(etl_config_path_to_run, self.app_config)
            except Exception as e:
                logging.critical(f"ETL pipeline failed for '{etl_config_path_to_run}'. Error: {e}", exc_info=True)
        else:
            logging.error(f"ETL mapping configuration file not found: {etl_config_path_to_run}")

    def _handle_shrink_dataset_action(self, shrink_params_str):
        """Handles the --shrink_dataset command-line action."""
        logging.info(f"Shrink dataset action initiated with params: '{shrink_params_str}'")
        try:
            # Import shrink_dataset utility
            # Assuming shrink_dataset.py is in ETL and has necessary functions
            from ETL.shrink_dataset import shrink_json_lines_file, shrink_csv_file  # Or a unified function
        except ImportError as e:
            logging.critical(f"Could not import shrink_dataset utility. Error: {e}")
            return

        try:
            params = shrink_params_str.split(';')
            if len(params) != 3:
                logging.error("Invalid format for --shrink_dataset. Expected 'input_path;output_path;num_lines'.")
                return

            input_path_rel, output_path_rel, num_lines_str = params
            num_lines = int(num_lines_str)

            # Resolve paths relative to project root if not absolute
            project_root = self.registry.get_path('root', '.')  # Default to CWD if root not set

            input_path = input_path_rel if os.path.isabs(input_path_rel) else os.path.join(project_root, input_path_rel)
            output_path = output_path_rel if os.path.isabs(output_path_rel) else os.path.join(project_root,
                                                                                              output_path_rel)

            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            logging.info(f"Attempting to shrink '{input_path}' to '{output_path}' ({num_lines} lines).")

            if input_path.endswith(".csv") or input_path.endswith(".csv.gz"):
                shrink_csv_file(input_path, output_path, num_lines + 1)  # +1 for header in CSV
            elif input_path.endswith(".json") or input_path.endswith(".jsonl") or \
                    input_path.endswith(".json.gz") or input_path.endswith(".jsonl.gz"):
                shrink_json_lines_file(input_path, output_path, num_lines)  # Assumes JSON Lines
            else:
                logging.error(f"Unsupported file type for shrinking: {input_path}")
                return

            logging.info(f"Dataset shrinking complete. Output at: {output_path}")

        except ValueError:
            logging.error("Invalid number of lines for --shrink_dataset. Must be an integer.")
        except Exception as e:
            logging.critical(f"Error during dataset shrinking: {e}", exc_info=True)

    # def _handle_run_recommender_action(self):
    #     logging.info("Running recommender system...")
    #     # from Recommender.core import RecommenderSystem # Example
    #     # recommender = RecommenderSystem(self.app_config, self.registry)
    #     # recommender.run()
    #     print("Placeholder: Recommender system would run here.")

    # def _handle_augment_data_action(self):
    #     logging.info("Starting data augmentation...")
    #     # from Augmentation.pipeline import AugmentationPipeline # Example
    #     # aug_pipeline = AugmentationPipeline(self.app_config, self.registry)
    #     # aug_pipeline.start()
    #     print("Placeholder: Data augmentation would run here.")