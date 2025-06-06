import json
import csv
import os
import pymongo

from OLD.ETL.MongoDBConnection import MongoDBConnection
from OLD.Util.PathRegistry import PathRegistry


def convert_type(value, to_type):
    if to_type == "int":
        return int(value)
    elif to_type == "float":
        return float(value)
    elif to_type == "str":
        return str(value)
    elif to_type == "bool":
        return value.lower() in ("true", "1", "yes")
    return value

def load_csv(file_path, mapping):
    data = []
    with open(file_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            item = {}
            for src_field, props in mapping.items():
                dst_field = props['field']
                value = convert_type(row[src_field], props['type'])
                item[dst_field] = value
            data.append(item)
    return data

def load_json(file_path, mapping):
    with open(file_path, encoding='utf-8') as f:
        original_data = json.load(f)

    data = []
    for row in original_data:
        item = {}
        for src_field, props in mapping.items():
            dst_field = props['field']
            value = convert_type(row.get(src_field, None), props['type'])
            item[dst_field] = value
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
            print(f"Warning: No 'collections' key found in {etl_config_path}. Nothing to process.")
            return

        for collection_config in etl_config['collections']:
            file_name = collection_config['file']
            file_path = os.path.join(registry.get_path('raw_datasets_dir'), file_name)
            collection_name = collection_config['collection']
            mapping = collection_config['mapping']

            print(f"Processing file: {file_path} for collection: '{collection_name}'")

            ext = os.path.splitext(file_path)[1].lower()
            data = []

            if ext == '.csv':
                data = load_csv(file_path, mapping)
            elif ext == '.json':
                data = load_json(file_path, mapping)
            else:
                print(f"Error: Unsupported file format: {ext} for {file_name}")
                continue  # Skip to the next collection config

            collection = db[collection_name]  # Use the db object from the singleton
            if data:
                try:
                    # Check if the collection exists and if it's empty, or decide on update strategy
                    # For simplicity, this example inserts. You might want to upsert or replace.
                    collection.insert_many(data)
                    print(f"Inserted {len(data)} documents into collection '{collection_name}' from {file_name}")
                except pymongo.errors.BulkWriteError as e:
                    print(f"Error inserting documents into '{collection_name}': {e.details}")
                except Exception as e:
                    print(f"An unexpected error occurred during insertion for '{collection_name}': {e}")
            else:
                print(f"No data to insert for collection '{collection_name}' from {file_name}")

    except FileNotFoundError:
        print(f"Error: ETL config file '{etl_config_path}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in ETL config file '{etl_config_path}'.")
    except Exception as e:
        print(f"An error occurred during ETL process: {e}")

if __name__ == "__main__":
    registry = PathRegistry()
    registry.set_path('raw_datasets_dir', '/home/cristian/Documents/projects/pyCharm/tirocinio/downloaded_datasets/partial/')

    etl_config_file_path = '/etl_configurations/etl.json'
    config_file_path = '/config.json'
    run_etl(etl_config_file_path, config_file_path)