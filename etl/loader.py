import json
import csv
import os
import pymongo

from etl.MongoDBConnection import MongoDBConnection
from core.path_registry import PathRegistry
from utils.logger import LoggerManager

logger = LoggerManager().get_logger()


def convert_type(value, to_type, field_name="<unknown_field>"): # Added field_name for better logging
    if value is None: # If the source value is None (e.g., key missing in JSON or explicit null)
        if to_type == "null": # If the target type is explicitly 'null'
            return None
        # For other types, if source is None, we return None.
        # Avoids errors like int(None).
        # If you wanted "None" as a string for str type, handle it:
        # if to_type == "str": return "None"
        return None

    try:
        if to_type == "int":
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
            return None
        else:
            logger.warning(f"Field '{field_name}': Unsupported target type '{to_type}' for value '{value}'. Returning original value.")
            return value # Fallback for unrecognized types
    except (ValueError, TypeError) as e:
        logger.warning(f"Field '{field_name}': Error converting value '{value}' to type '{to_type}': {e}. Returning None.")
        return None

def load_csv(file_path, mapping):
    data = []
    with open(file_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader): # Added enumerate for row number in logging if needed
            item = {}
            for src_field, props in mapping.items():
                dst_field = props['field']
                target_type = props['type']
                if src_field in row:
                    raw_value = row[src_field]
                    # Pass dst_field for logging context, as it's the field name in the target schema
                    converted_value = convert_type(raw_value, target_type, field_name=dst_field)
                    item[dst_field] = converted_value
                else:
                    logger.warning(f"CSV Row {i+1}: Source field '{src_field}' not found in file '{file_path}'. Skipping for field '{dst_field}'.")
                    # Handle missing field: either skip or assign None after conversion
                    item[dst_field] = convert_type(None, target_type, field_name=dst_field)
            data.append(item)
    return data

def load_json(file_path, mapping):
    with open(file_path, encoding='utf-8') as f:
        # Determine if it's a list of objects or a single object per line (NDJSON)
        # This simple check tries to load the whole thing first.
        # For very large NDJSON, you'd read line by line.
        try:
            content = f.read()
            original_data_source = json.loads(content)
        except json.JSONDecodeError:
            # Try parsing as NDJSON
            f.seek(0) # Reset file pointer
            original_data_source = []
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    original_data_source.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON line {line_num+1} in '{file_path}': {line[:100]}...")
                    continue # Skip malformed line
            logger.info(f"Loaded '{file_path}' as NDJSON.")


    # Ensure original_data_source is a list to iterate over
    if not isinstance(original_data_source, list):
        if isinstance(original_data_source, dict):
            original_data_source = [original_data_source] # Handle case where JSON is a single object
            logger.info(f"'{file_path}' contained a single JSON object; processing as a list of one.")
        else:
            logger.error(f"Data in '{file_path}' is not a list of objects or a single object. Type: {type(original_data_source)}")
            return []


    data = []
    for i, row in enumerate(original_data_source): # Added enumerate for row number in logging
        if not isinstance(row, dict):
            logger.warning(f"JSON Row {i+1} in '{file_path}' is not a dictionary. Skipping row. Data: {str(row)[:100]}")
            continue
        item = {}
        for src_field, props in mapping.items():
            dst_field = props['field']
            target_type = props['type']
            raw_value = row.get(src_field) # Default is None if key is missing
            # Pass dst_field for logging context
            converted_value = convert_type(raw_value, target_type, field_name=dst_field)
            item[dst_field] = converted_value
        data.append(item)
    return data

def run_etl(etl_config_path, config_file_path=None):
    registry = PathRegistry()
    if config_file_path:
        registry.set_path('config_file', config_file_path)
    mongo_conn = MongoDBConnection(registry.get_path('config_file'))
    db = mongo_conn.get_database()

    try:
        # Load the ETL configuration from the provided path
        with open(etl_config_path, 'r', encoding='utf-8') as f:
            etl_config = json.load(f)

        if 'collections' not in etl_config:
            logger.error(f"Warning: No 'collections' key found in {etl_config_path}. Nothing to process.")
            return

        for collection_config in etl_config['collections']:
            file_name = collection_config['file']
            file_path = os.path.join(registry.get_path('raw_datasets_dir'), file_name)
            collection_name = collection_config['collection']
            mapping = collection_config['mapping']

            logger.debug(f"Processing file: {file_path} for collection: '{collection_name}'")

            ext = os.path.splitext(file_path)[1].lower()
            data = []

            if ext == '.csv':
                data = load_csv(file_path, mapping)
            elif ext == '.json':
                data = load_json(file_path, mapping)
            else:
                logger.error(f"Error: Unsupported file format: {ext} for {file_name}")
                continue  # Skip to the next collection config

            collection = db[collection_name]  # Use the db object from the singleton
            if data:
                try:
                    # Check if the collection exists and if it's empty, or decide on update strategy
                    # For simplicity, this example inserts. You might want to upsert or replace.
                    collection.insert_many(data)
                    logger.info(f"Inserted {len(data)} documents into collection '{collection_name}' from {file_name}")
                except pymongo.errors.BulkWriteError as e:
                    logger.error(f"Error inserting documents into '{collection_name}': {e.details}")
                except Exception as e:
                    logger.error(f"An unexpected error occurred during insertion for '{collection_name}': {e}")
            else:
                logger.error(f"No data to insert for collection '{collection_name}' from {file_name}")

    except FileNotFoundError:
        logger.error(f"Error: ETL config file '{etl_config_path}' not found.")
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON in ETL config file '{etl_config_path}'.")
    except Exception as e:
        logger.error(f"An error occurred during ETL process: {e}")

def exec_all_etl(path_list, config_file_path=None):
    """
    Executes ETL processes for all paths in the provided list.
    :param path_list: List of ETL configuration file paths.
    :param config_file_path: Optional path to the main configuration file.
    """
    for etl_config_path in path_list:
        logger.info(f"Running ETL for config: {etl_config_path}")
        run_etl(etl_config_path, config_file_path)

if __name__ == "__main__":
    registry = PathRegistry()
    registry.set_path('raw_datasets_dir', '/home/cristian/Documents/projects/pyCharm/tirocinio/downloaded_datasets/partial/')

    etl_config_file_path = '/etl_configurations/etl.json'
    config_file_path = '/config.json'
    run_etl(etl_config_file_path, config_file_path)