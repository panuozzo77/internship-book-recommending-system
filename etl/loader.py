#etl/loader.py
import json
import csv
import os
import pymongo
from typing import Dict, Any, Generator, List # Import necessary types

from etl.MongoDBConnection import MongoDBConnection
from core.path_registry import PathRegistry
from utils.logger import LoggerManager

logger_manager = LoggerManager()


def convert_type(value, to_type, field_name="<unknown_field>"):
    logger = logger_manager.get_logger()
    # ... (convert_type function remains the same) ...
    if value is None: # If the source value is None (e.g., key missing in JSON or explicit null)
        if to_type == "null": # If the target type is explicitly 'null'
            return None
        return None # Return None for other types if source is None

    try:
        if to_type == "int":
            # Handle cases where value might be float string like "1.0"
            if isinstance(value, str) and '.' in value:
                 return int(float(value))
            return int(value)
        elif to_type == "float":
            return float(value)
        elif to_type == "str":
            return str(value)
        elif to_type == "bool":
            if isinstance(value, bool): # If already a boolean (from JSON)
                return value
            # For string inputs (from CSV or even JSON if bools are strings)
            return str(value).lower() in ("true", "1", "t", "yes", "y")
        elif to_type == "list":
            if isinstance(value, list): # Already a list (primarily from JSON)
                return value
            elif isinstance(value, str): # String representation (primarily from CSV)
                try:
                    parsed_value = json.loads(value)
                    if isinstance(parsed_value, list):
                        return parsed_value
                    else:
                        logger.warning(f"Field '{field_name}': Value '{value}' parsed but is not a list. Returning None.")
                        return None
                except json.JSONDecodeError:
                    logger.warning(f"Field '{field_name}': Could not parse string '{value}' as a JSON list. Returning None.")
                    return None
            else:
                logger.warning(f"Field '{field_name}': Value '{value}' (type: {type(value)}) cannot be converted to list. Returning None.")
                return None
        elif to_type == "dict":
            if isinstance(value, dict): # Already a dict (primarily from JSON)
                return value
            elif isinstance(value, str): # String representation (primarily from CSV)
                try:
                    parsed_value = json.loads(value)
                    if isinstance(parsed_value, dict):
                        return parsed_value
                    else:
                        logger.warning(f"Field '{field_name}': Value '{value}' parsed but is not a dict. Returning None.")
                        return None
                except json.JSONDecodeError:
                    logger.warning(f"Field '{field_name}': Could not parse string '{value}' as a JSON dict. Returning None.")
                    return None
            else:
                logger.warning(f"Field '{field_name}': Value '{value}' (type: {type(value)}) cannot be converted to dict. Returning None.")
                return None
        elif to_type == "null": # If type is 'null', ensure None is returned
             if value is None: # Only map explicit None to None
                 return None
             else:
                 logger.warning(f"Field '{field_name}': Value '{value}' is not None, but target type is 'null'. Returning None.")
                 return None
        else:
            logger.warning(f"Field '{field_name}': Unsupported target type '{to_type}' for value '{value}'. Returning original value.")
            return value # Fallback for unrecognized types
    except (ValueError, TypeError) as e:
        logger.warning(f"Field '{field_name}': Error converting value '{value}' to type '{to_type}': {e}. Returning None.")
        return None


# Modified to be a generator
def load_csv_items(file_path: str, mapping: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    logger = logger_manager.get_logger()
    try:
        with open(file_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                item = {}
                for src_field, props in mapping.items():
                    dst_field = props['field']
                    target_type = props['type']
                    if src_field in row:
                        raw_value = row[src_field]
                        converted_value = convert_type(raw_value, target_type, field_name=dst_field)
                        item[dst_field] = converted_value
                    else:
                        # Handle missing field: either skip or assign None after conversion
                        # Assigning None is safer if schema expects the field
                        item[dst_field] = convert_type(None, target_type, field_name=dst_field)

                # Yield the processed item
                yield item
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
    except Exception as e:
        logger.error(f"An error occurred while reading or processing CSV file '{file_path}': {e}", exc_info=True)


# Modified to be a generator, handling standard JSON arrays and NDJSON line-by-line
def load_json_items(file_path: str, mapping: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    logger = logger_manager.get_logger()
    try:
        with open(file_path, encoding='utf-8') as f:
            # Attempt to load as a single JSON document (list or object) first
            try:
                original_data_source = json.load(f)
                logger.debug(f"Loaded '{file_path}' as standard JSON.")
                # If it's a list, iterate it. If it's a single object, wrap it in a list.
                if not isinstance(original_data_source, list):
                     if isinstance(original_data_source, dict):
                         original_data_source = [original_data_source]
                         logger.info(f"'{file_path}' contained a single JSON object; processing as a list of one.")
                     else:
                         logger.error(f"Data in '{file_path}' is not a list or single object. Type: {type(original_data_source)}. Cannot process.")
                         return # Exit generator
                # Now original_data_source is guaranteed to be a list (or empty)
                for i, row in enumerate(original_data_source):
                    if not isinstance(row, dict):
                        logger.warning(f"JSON Item {i+1} in '{file_path}' is not a dictionary. Skipping item. Data: {str(row)[:100]}")
                        continue
                    item = {}
                    for src_field, props in mapping.items():
                        dst_field = props['field']
                        target_type = props['type']
                        raw_value = row.get(src_field)
                        converted_value = convert_type(raw_value, target_type, field_name=dst_field)
                        item[dst_field] = converted_value
                    yield item # Yield individual item

            except json.JSONDecodeError:
                # If standard JSON loading fails, try parsing as NDJSON (one JSON object per line)
                logger.info(f"Standard JSON decode failed for '{file_path}'. Attempting NDJSON.")
                f.seek(0) # Reset file pointer
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        if not isinstance(row, dict):
                            logger.warning(f"NDJSON Line {line_num+1} in '{file_path}' is not a dictionary. Skipping line. Data: {line[:100]}")
                            continue

                        item = {}
                        for src_field, props in mapping.items():
                            dst_field = props['field']
                            target_type = props['type']
                            raw_value = row.get(src_field)
                            converted_value = convert_type(raw_value, target_type, field_name=dst_field)
                            item[dst_field] = converted_value
                        yield item # Yield individual item

                    except json.JSONDecodeError:
                        logger.error(f"Error decoding NDJSON line {line_num+1} in '{file_path}': {line[:100]}...")
                        # Skip malformed line, generator continues
                logger.info(f"Finished processing '{file_path}' as NDJSON.")

    except FileNotFoundError:
        logger.error(f"JSON file not found: {file_path}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while opening or processing JSON file '{file_path}': {e}", exc_info=True)


def run_etl(etl_config_path: str, app_config: Dict[str, Any], registry: PathRegistry) -> None:
    # Get logger from manager
    logger = logger_manager.get_logger()

    # MongoDB connection using the singleton manager
    mongo_conn = MongoDBConnection() # MongoDBConnection is a singleton, doesn't need config path on every call
    db = mongo_conn.get_database()

    try:
        # Load the ETL configuration from the provided path
        with open(etl_config_path, 'r', encoding='utf-8') as f:
            etl_config = json.load(f)

        if 'collections' not in etl_config:
            logger.error(f"Error: No 'collections' key found in {etl_config_path}. Nothing to process.")
            return

        for collection_config in etl_config['collections']:
            # Get config values
            file_name = collection_config.get('file')
            collection_name = collection_config.get('collection')
            mapping = collection_config.get('mapping')
            # Get chunk_size from config, with a reasonable default (e.g., 1000 documents)
            chunk_size = collection_config.get('chunk_size', 1000)

            # Basic validation
            if not file_name or not collection_name or not mapping:
                 logger.error(f"Skipping collection entry in {etl_config_path} due to missing 'file', 'collection', or 'mapping'. Entry: {collection_config}")
                 continue
            if not isinstance(chunk_size, int) or chunk_size <= 0:
                 logger.warning(f"Invalid chunk_size ({chunk_size}) for collection '{collection_name}'. Using default 1000.")
                 chunk_size = 1000

            logger.info(f"Processing file: {file_name} for collection: '{collection_name}' with chunk size {chunk_size}")

            # Construct file path
            raw_datasets_dir = registry.get_path('raw_datasets_dir')
            if not raw_datasets_dir:
                logger.error(f"Path for 'raw_datasets_dir' not found in PathRegistry. Cannot process {file_name} for '{collection_name}'.")
                continue # Skip to the next collection config

            file_path = os.path.join(raw_datasets_dir, file_name)

            # Determine which generator function to use
            ext = os.path.splitext(file_path)[1].lower()
            item_generator = None # Will hold the generator yielding individual items

            if ext == '.csv':
                item_generator = load_csv_items(file_path, mapping)
            elif ext == '.json':
                 # load_json_items handles both standard JSON and NDJSON
                item_generator = load_json_items(file_path, mapping)
            else:
                logger.error(f"Error: Unsupported file format: {ext} for {file_name}. Skipping collection '{collection_name}'.")
                continue  # Skip to the next collection config

            # Get the MongoDB collection object
            collection = db[collection_name]

            # --- Process the generator in chunks and insert ---
            chunk: List[Dict[str, Any]] = [] # Initialize empty chunk buffer
            total_documents_inserted = 0
            chunk_count = 0

            for item in item_generator: # Iterate over the generator (yields individual documents)
                if item: # Ensure item is not None or empty if your mapping could result in that
                    chunk.append(item)
                    if len(chunk) >= chunk_size:
                        # Insert the current chunk
                        try:
                            if chunk: # Defensive check, should not be empty if len >= chunk_size > 0
                                insert_result = collection.insert_many(chunk, ordered=False) # ordered=False allows some inserts to succeed if others fail
                                logger.debug(f"Inserted chunk {chunk_count + 1} ({len(chunk)} documents) into '{collection_name}'.")
                                total_documents_inserted += len(insert_result.inserted_ids) # Count successful inserts
                                chunk_count += 1
                                chunk = [] # Clear the chunk buffer for the next batch
                        except pymongo.errors.BulkWriteError as bwe:
                             # Log details of documents that failed
                             logger.error(f"BulkWriteError inserting chunk {chunk_count + 1} into '{collection_name}'. Errors:")
                             for error in bwe.details.get('writeErrors', []):
                                 # Attempt to log information about the failing document if possible
                                 # The exact document data is often not in the error details,
                                 # you might need more sophisticated error handling if identifying failing docs is critical.
                                 logger.error(f"  Index: {error.get('index')}, Code: {error.get('code')}, Message: {error.get('errmsg')}")
                             total_documents_inserted += bwe.details.get('nInserted', 0) # Count successful inserts in this partial batch
                             chunk_count += 1 # Still count as a chunk attempt
                             chunk = [] # Clear the chunk buffer
                        except Exception as e:
                            logger.error(f"An unexpected error occurred during insertion of chunk {chunk_count + 1} for '{collection_name}': {e}", exc_info=True)
                            # Decide how to handle: clear chunk and continue, or stop?
                            # Clearing and continuing is often appropriate for large imports
                            chunk_count += 1
                            chunk = [] # Clear the chunk buffer

            # --- Insert any remaining documents in the last chunk ---
            if chunk: # If the last chunk is not empty
                 try:
                     insert_result = collection.insert_many(chunk, ordered=False)
                     logger.debug(f"Inserted final chunk ({len(chunk)} documents) into '{collection_name}'.")
                     total_documents_inserted += len(insert_result.inserted_ids)
                     chunk_count += 1
                 except pymongo.errors.BulkWriteError as bwe:
                     logger.error(f"BulkWriteError inserting final chunk into '{collection_name}'. Errors:")
                     for error in bwe.details.get('writeErrors', []):
                         logger.error(f"  Index: {error.get('index')}, Code: {error.get('code')}, Message: {error.get('errmsg')}")
                     total_documents_inserted += bwe.details.get('nInserted', 0)
                     chunk_count += 1
                 except Exception as e:
                    logger.error(f"An unexpected error occurred during insertion of the final chunk for '{collection_name}': {e}", exc_info=True)
                    chunk_count += 1


            logger.info(f"Finished processing file: {file_name}. Total documents inserted into '{collection_name}': {total_documents_inserted}")

    except FileNotFoundError:
        logger.error(f"Error: ETL config file '{etl_config_path}' not found.")
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON in ETL config file '{etl_config_path}'.")
    except Exception as e:
        logger.error(f"An error occurred during ETL process: {e}", exc_info=True)

def exec_all_etl(path_list: List[str], app_config: Dict[str, Any], registry: PathRegistry) -> None:
    """
    Executes ETL processes for all paths in the provided list.
    :param path_list: List of ETL configuration file paths.
    :param app_config: The loaded application configuration dictionary.
    :param registry: The PathRegistry instance.
    """
    logger = logger_manager.get_logger()
    logger.info(f"Executing ETL for {len(path_list)} config files.")
    for etl_config_path in path_list:
        logger.info(f"Running ETL for config: {etl_config_path}")
        # Pass app_config and registry to run_etl
        run_etl(etl_config_path, app_config, registry)
    logger.info("Finished executing all ETL configurations.")


if __name__ == "__main__":
    # Example usage within a script - needs appropriate setup
    # This __main__ block is simplified for demonstration and might not run
    # correctly without the full application initialization context.
    # In your main application flow (e.g., in core/app_initializer.py),
    # exec_all_etl is called with the correct app_config and registry.
    logger_manager.setup_logger("MainLogger", level="INFO") # Setup a basic logger for testing
    logger = logger_manager.get_logger()
    logger.info("Running ETL example from __main__.")

    # Simulate parts of app_initializer setup
    registry = PathRegistry()
    # Make sure this path exists and contains your raw data
    raw_datasets_dir_example = '/path/to/your/downloaded_datasets/partial/'
    registry.set_path('raw_datasets_dir', raw_datasets_dir_example)

    # Simulate a simple app_config with a chunk_size
    app_config_example = {
        "logging": {"name": "MainLogger", "level": "INFO"}, # Ensure logger is setup
        "etl_list": ["etl_example.json"] # Point to your example config file
    }

    # Create a dummy etl_example.json for testing
    # In a real scenario, this file would exist and be loaded
    dummy_etl_config_content = {
        "collections": [
            {
                "file": "your_large_file.json", # Replace with a real file name
                "collection": "your_test_collection", # Replace with a test collection name
                "mapping": { # Replace with your actual mapping
                    "source_field_1": {"field": "dest_field_1", "type": "str"},
                    "source_field_2": {"field": "dest_field_2", "type": "int"}
                },
                "chunk_size": 500 # Example chunk size for this collection
            },
            # Add other collections as needed
        ]
    }

    # You would typically load the real app config and etl configs
    # For this example, let's assume etl.json exists or you create a dummy one
    # etl_config_path_example = os.path.join(registry.get_path('config_dir', '.'), app_config_example['etl_list'][0])
    # For a simple test, create a dummy file path
    etl_config_path_example = "./etl_example_test.json"
    try:
        with open(etl_config_path_example, 'w') as f:
             json.dump(dummy_etl_config_content, f, indent=4)
        logger.info(f"Created dummy ETL config: {etl_config_path_example}")

        # Simulate execution
        exec_all_etl([etl_config_path_example], app_config_example, registry)

    except Exception as e:
        logger.error(f"Error during __main__ execution: {e}", exc_info=True)
    finally:
        # Clean up dummy file
        if os.path.exists(etl_config_path_example):
             os.remove(etl_config_path_example)
             logger.info(f"Removed dummy ETL config: {etl_config_path_example}")