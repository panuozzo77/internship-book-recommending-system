# Util/ArgHandler.py
import argparse
import json
import logging
import os

from ETL2.etl_loader import run_etl


# PathRegistry and APP_CONFIG are passed via __init__

class ArgHandler:
    def __init__(self, app_config, path_registry):
        self.app_config = app_config
        self.registry = path_registry
        self.parser = self._create_parser()
        self.args = None

    def _create_parser(self):
        project_name = self.app_config.get("project_name", "Application")
        parser = argparse.ArgumentParser(description=f"{project_name} Command Line Interface")
        parser.add_argument(
            "--config", type=str,
            help="Path to a custom application configuration file (e.g., config.json)."
        )
        # --- MODIFIED --load_etl to be a flag ---
        parser.add_argument(
            "--load_etl",
            action="store_true",  # This makes it a flag; present means True, absent means False
            help="Run all ETL processes defined in the main application config's 'etl_list'."
        )
        parser.add_argument(
            "--shrink_dataset",
            type=str,
            metavar="INPUT_PATH;OUTPUT_PATH;NUM_LINES",
            help="Shrinks a dataset. Provide input_path, output_path, and num_lines separated by semicolons."
        )
        # Add other arguments here
        return parser

    def parse_arguments(self, custom_args_list=None):
        self.args = self.parser.parse_args(args=custom_args_list)
        logging.debug(f"CLI arguments parsed: {self.args}")
        return self.args

    def handle_actions(self):
        if not self.args:
            logging.warning("Arguments not parsed yet. Call parse_arguments() first.")
            return

        # --- MODIFIED ETL Loading Action ---
        if self.args.load_etl:  # Now this is True if the flag is present
            self._handle_configured_etl_load_action()  # New method name for clarity

        if self.args.shrink_dataset:
            self._handle_shrink_dataset_action(self.args.shrink_dataset)

        # Check if any recognized action was triggered
        action_triggered = self.args.load_etl or self.args.shrink_dataset  # Add other actions here
        if not action_triggered:
            logging.info(
                "No specific action requested via CLI arguments for ArgHandler (or action not yet implemented).")

    def _handle_configured_etl_load_action(self):
        """
        Handles the --load_etl flag by processing all ETL mapping files
        listed in the main application config's 'etl_list'.
        """
        logging.info("ETL load action initiated (processing all configured ETLs).")

        etl_list_from_config = self.app_config.get("etl_list", [])
        etl_configs_base_dir = self.registry.get_path('etl_configs_dir')
        print(self.registry.all_paths())  # Debugging line to check all paths in registry

        if not etl_list_from_config:
            logging.warning("No ETL mapping files listed in 'etl_list' in the main configuration.")
            return
        if not etl_configs_base_dir:
            logging.error("Path for 'etl_configs_dir' not found in PathRegistry. Cannot locate ETL mapping files.")
            return

        logging.info(f"Found {len(etl_list_from_config)} ETL configurations to process: {etl_list_from_config}")

        for etl_mapping_filename in etl_list_from_config:
            etl_mapping_config_path = os.path.join(etl_configs_base_dir, etl_mapping_filename)
            logging.info(f"Processing ETL mapping file: {etl_mapping_config_path}")
            run_etl(etl_mapping_config_path)
            ''' 
            if os.path.exists(etl_mapping_config_path):
                logging.info(f"Executing Simple ETL with mapping config: {etl_mapping_config_path}")
                try:
                    with open(etl_mapping_config_path, 'r', encoding='utf-8') as f:
                        etl_mapping_config_data = json.load(f)

                    # Pass the loaded mapping config, the main app config, and registry
                    
                    logging.info(f"Finished processing ETL: {etl_mapping_filename}")
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding ETL mapping JSON from {etl_mapping_config_path}: {e}")
                except Exception as e:
                    logging.critical(f"Simple ETL pipeline failed for '{etl_mapping_config_path}'. Error: {e}",
                                     exc_info=True)
            else:
                logging.error(f"Simple ETL mapping configuration file not found: {etl_mapping_config_path}")
        '''
        logging.info("All configured ETL processes (if any) have been attempted.")

    def _handle_shrink_dataset_action(self, shrink_params_str):
        # ... (implementation from previous answer is fine) ...
        logging.info(f"Shrink dataset action initiated with params: '{shrink_params_str}'")
        try:
            from ETL.shrink_dataset import shrink_json_lines_file, shrink_csv_file
        except ImportError as e:
            logging.critical(f"Could not import shrink_dataset utility. Error: {e}")
            return

        try:
            params = shrink_params_str.split(';')
            if len(params) != 3:
                logging.error("Invalid format for --shrink_dataset. Expected 'INPUT_PATH;OUTPUT_PATH;NUM_LINES'.")
                return

            input_path_rel, output_path_rel, num_lines_str = params
            num_lines = int(num_lines_str)
            project_root = self.registry.get_path('root', '.')
            input_path = input_path_rel if os.path.isabs(input_path_rel) else os.path.join(project_root, input_path_rel)
            output_path = output_path_rel if os.path.isabs(output_path_rel) else os.path.join(project_root,
                                                                                              output_path_rel)

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            logging.info(f"Attempting to shrink '{input_path}' to '{output_path}' ({num_lines} lines).")

            if input_path.endswith((".csv", ".csv.gz")):
                shrink_csv_file(input_path, output_path, num_lines + 1)
            elif input_path.endswith((".json", ".jsonl", ".json.gz", ".jsonl.gz")):
                shrink_json_lines_file(input_path, output_path, num_lines)
            else:
                logging.error(f"Unsupported file type for shrinking: {input_path}")
                return
            logging.info(f"Dataset shrinking complete. Output at: {output_path}")
        except ValueError:
            logging.error("Invalid number of lines for --shrink_dataset. Must be an integer.")
        except Exception as e:
            logging.critical(f"Error during dataset shrinking: {e}", exc_info=True)