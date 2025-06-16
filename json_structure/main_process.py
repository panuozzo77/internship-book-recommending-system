import json
from pathlib import Path
from json_schema_util import generate_structure_json # Make sure this file exists and is importable

def process_all_json_in_directory(input_dir_path_str: str,
                                  output_dir_path_str: str = None,
                                  aggregate_output_file_path_str: str = None):
    """
    Processes all JSON files in a given directory to generate their structure schemas.

    Args:
        input_dir_path_str (str): Path to the directory containing JSON files.
        output_dir_path_str (str, optional): Path to a directory where individual
                                             schema files will be saved. If None,
                                             schemas are not saved individually.
        aggregate_output_file_path_str (str, optional): Path to a single JSON file
                                                        where all generated schemas
                                                        will be aggregated. If None,
                                                        no aggregated file is created.
    """
    input_dir = Path(input_dir_path_str)
    if not input_dir.is_dir():
        print(f"Error: Input directory '{input_dir}' not found or is not a directory.")
        return

    all_collections_for_aggregation = []

    # Prepare output directory for individual files if specified
    if output_dir_path_str:
        output_dir = Path(output_dir_path_str)
        output_dir.mkdir(parents=True, exist_ok=True) # Create if it doesn't exist
        print(f"Saving individual schemas to: {output_dir.resolve()}")

    # Iterate through .json files (and .jsonl for newline-delimited JSON)
    # You can adjust the glob pattern as needed
    file_patterns = ['*.json', '*.jsonl']
    found_files = []
    for pattern in file_patterns:
        found_files.extend(list(input_dir.glob(pattern)))

    if not found_files:
        print(f"No JSON or JSONL files found in '{input_dir}'.")
        return

    print(f"\nFound {len(found_files)} files to process in '{input_dir}'.")

    for file_path in found_files:
        print(f"\n--- Processing: {file_path.name} ---")

        # The collection_name will be derived from the filename by default
        # inside generate_structure_json if not provided.
        # Or, you could customize it here, e.g., collection_name=file_path.stem
        schema = generate_structure_json(str(file_path))

        if schema:
            print(f"Successfully generated schema for {file_path.name}.")
            # print(json.dumps(schema, indent=2)) # Optionally print each schema

            # Save individual schema file
            if output_dir_path_str:
                output_schema_filename = f"{file_path.stem}_schema.json"
                individual_output_path = output_dir / output_schema_filename
                try:
                    with open(individual_output_path, 'w', encoding='utf-8') as outfile:
                        json.dump(schema, outfile, indent=2)
                    print(f"Saved schema to: {individual_output_path}")
                except IOError as e:
                    print(f"Error saving individual schema for {file_path.name}: {e}")


            # Add to aggregation list
            if aggregate_output_file_path_str:
                if "collections" in schema and isinstance(schema["collections"], list):
                    # The schema for one file has one item in its "collections" list
                    all_collections_for_aggregation.extend(schema["collections"])
                else:
                    print(f"Warning: Schema for {file_path.name} did not have the expected 'collections' list structure.")
        else:
            print(f"Could not generate schema for {file_path.name}.")

    # Save aggregated schema file
    if aggregate_output_file_path_str and all_collections_for_aggregation:
        aggregated_schema_data = {"collections": all_collections_for_aggregation}
        agg_output_path = Path(aggregate_output_file_path_str)
        try:
            # Ensure parent directory for aggregated file exists
            agg_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(agg_output_path, 'w', encoding='utf-8') as agg_outfile:
                json.dump(aggregated_schema_data, agg_outfile, indent=2)
            print(f"\nSuccessfully saved aggregated schemas to: {agg_output_path.resolve()}")
        except IOError as e:
            print(f"Error saving aggregated schema file: {e}")
    elif aggregate_output_file_path_str:
        print("\nNo schemas were generated or successfully processed for aggregation.")


if __name__ == "__main__":
    # --- Configuration ---
    # Relative path from where this script is run
    input_directory = "../downloaded_datasets/partial/"

    # Option 1: Save each schema to its own file in a specified output directory
    individual_schemas_output_dir = "./generated_schemas_individual/"

    # Option 2: Aggregate all schemas into one big JSON file
    aggregated_schema_file = "./generated_schemas_aggregated/all_dataset_schemas.json"

    # --- Run the processing ---
    print(f"Starting schema generation for files in: {Path(input_directory).resolve()}")

    process_all_json_in_directory(
        input_directory,
        output_dir_path_str=individual_schemas_output_dir,
        aggregate_output_file_path_str=aggregated_schema_file
    )

    print("\nProcessing complete.")

    # To run with only individual outputs:
    # process_all_json_in_directory(input_directory, output_dir_path_str=individual_schemas_output_dir)

    # To run with only aggregated output:
    # process_all_json_in_directory(input_directory, aggregate_output_file_path_str=aggregated_schema_file)

    # To only print (no saving), call with default None for output paths:
    # process_all_json_in_directory(input_directory)