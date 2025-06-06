# ETL/mongodb_mapper.py
import logging
import pandas as pd
from datetime import datetime
from .MongoDBConnection import DatabaseManager  # Relative import

logger = logging.getLogger(__name__)


# --- Transformation Functions ---

def _combine_date_parts(row_series: pd.Series, source_columns_config: list, mapping_rule: dict):
    """Combines year, month, day from specified source columns into a datetime object."""
    try:
        year_col, month_col, day_col = source_columns_config
        # Get values, coerce errors to NaT/NaN for numeric conversion
        year = pd.to_numeric(row_series.get(year_col), errors='coerce')
        month = pd.to_numeric(row_series.get(month_col), errors='coerce')
        day = pd.to_numeric(row_series.get(day_col), errors='coerce')

        if pd.isna(year):
            return None  # Year is mandatory

        # Default month and day to 1 if not present or invalid
        month_int = int(month) if pd.notna(month) and month > 0 and month <= 12 else 1
        day_int = int(day) if pd.notna(day) and day > 0 and day <= 31 else 1

        # Basic validation for day based on month (simplified)
        if month_int in [4, 6, 9, 11] and day_int > 30: day_int = 30
        if month_int == 2:
            is_leap = (int(year) % 4 == 0 and int(year) % 100 != 0) or (int(year) % 400 == 0)
            if is_leap and day_int > 29:
                day_int = 29
            elif not is_leap and day_int > 28:
                day_int = 28

        return datetime(int(year), month_int, day_int)
    except Exception as e:
        # Log specific row identifier if possible, row_series.name might be the index
        row_identifier = getattr(row_series, 'name', 'N/A')
        logger.warning(f"Could not combine date parts for row '{row_identifier}' "
                       f"using cols {source_columns_config}. Error: {e}. Rule: {mapping_rule}")
        return None


def _transform_list_of_objects(source_list_data, object_mapping_config: list, mapping_rule: dict):
    """Transforms a list of source objects based on object_mapping_config."""
    if not isinstance(source_list_data, list):
        if pd.isna(source_list_data): return []  # Treat NaN as empty list
        logger.debug(
            f"Source for list_of_objects is not a list: {source_list_data} (Rule: {mapping_rule.get('target_field')}). Returning empty list.")
        return []

    transformed_list = []
    for item in source_list_data:
        if not isinstance(item, dict):
            logger.debug(
                f"Item in list_of_objects is not a dict: {item} (Rule: {mapping_rule.get('target_field')}). Skipping item.")
            continue

        transformed_item = {}
        for sub_mapping in object_mapping_config:
            source_key = sub_mapping['source_key']
            target_key = sub_mapping['target_key']
            target_sub_type = sub_mapping.get('type', 'string')  # Type for sub-field

            if source_key in item:
                raw_sub_value = item[source_key]
                # Apply casting to sub-item value
                transformed_item[target_key] = _cast_value(raw_sub_value, target_sub_type,
                                                           sub_mapping.get('default_value'))

        if transformed_item:  # Only add if it has some mapped data
            transformed_list.append(transformed_item)
    return transformed_list


# Register transformation functions
TRANSFORMATIONS = {
    "combine_date_parts": _combine_date_parts,
    # Add more custom transformation identifiers here
}


def _cast_value(value, target_type_str: str, default_value=None):
    """Casts a value to the target Python type, suitable for MongoDB."""
    if value is None or (isinstance(value, float) and pd.isna(value)):  # Handle None and pandas.NA/NaN
        return default_value

    original_value_for_log = value  # Keep original for logging if cast fails

    try:
        if target_type_str == "string": return str(value)
        if target_type_str == "integer":
            # Handle potential floats from pandas if they are whole numbers
            if isinstance(value, float) and value.is_integer():
                return int(value)
            return int(value)  # Will raise ValueError if not convertible (e.g., "abc")
        if target_type_str == "float": return float(value)
        if target_type_str == "boolean":
            if isinstance(value, str):
                return value.lower() in ['true', '1', 't', 'yes', 'y']
            return bool(value)
        if target_type_str == "date":  # Expects datetime object or ISO string
            if isinstance(value, datetime): return value
            if isinstance(value, str): return datetime.fromisoformat(value.replace("Z", "+00:00"))  # Handle Z for UTC
            logger.warning(f"Cannot cast type {type(value)} to date. Value: {original_value_for_log}")
            return default_value
        if target_type_str == "list_of_strings":  # Example for simple list
            if isinstance(value, list) and all(isinstance(x, str) for x in value): return value
            if isinstance(value, str): return [s.strip() for s in value.split(',') if
                                               s.strip()]  # Basic CSV string to list
            logger.warning(f"Cannot cast to list_of_strings: {original_value_for_log}")
            return default_value if default_value is not None else []
        # "list_of_objects" is handled by _transform_list_of_objects directly
        # Add more complex types like "list_of_integers" as needed
        logger.debug(f"No specific cast for type '{target_type_str}'. Returning value as is: {value}")
        return value  # Fallback for types not explicitly handled or for direct pass-through
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not cast value '{original_value_for_log}' (type: {type(original_value_for_log)}) "
                       f"to type '{target_type_str}'. Using default: {default_value}. Error: {e}")
        return default_value


def map_row_to_document(row_series: pd.Series, document_structure_config: list):
    """Maps a Pandas DataFrame row (Series) to a MongoDB document based on mapping rules."""
    mongo_doc = {}
    for mapping_rule in document_structure_config:
        target_field = mapping_rule['target_field']
        target_type = mapping_rule.get('type', 'string')  # Default to string
        default_val_for_rule = mapping_rule.get('default_value')  # Default for this specific rule

        value_to_set = None

        if 'transform' in mapping_rule:
            transform_func_name = mapping_rule['transform']
            transform_func = TRANSFORMATIONS.get(transform_func_name)
            if transform_func:
                source_cols_for_transform = mapping_rule.get('source_columns', [])
                # Pass the entire row and the specific config for source_columns for that transform
                value_to_set = transform_func(row_series, source_cols_for_transform, mapping_rule)
            else:
                logger.warning(
                    f"Unknown transformation function '{transform_func_name}' for target field '{target_field}'.")
                value_to_set = default_val_for_rule  # Use rule's default if transform fails to find
        elif 'source_column' in mapping_rule:
            source_col_name = mapping_rule['source_column']
            raw_value_from_df = row_series.get(source_col_name)  # Get value from DataFrame row

            if target_type == 'list_of_objects' and 'object_mapping' in mapping_rule:
                value_to_set = _transform_list_of_objects(raw_value_from_df, mapping_rule['object_mapping'],
                                                          mapping_rule)
            else:
                value_to_set = _cast_value(raw_value_from_df, target_type, default_val_for_rule)
        elif 'value' in mapping_rule:  # For setting a static value
            value_to_set = _cast_value(mapping_rule['value'], target_type, default_val_for_rule)
        else:  # Field might be intended to be populated later or has no direct source/transform
            value_to_set = default_val_for_rule  # Use rule's default

        # Only add field to document if it's not None,
        # or if the rule explicitly says to include nulls (not implemented here, but could be an option)
        if value_to_set is not None:
            mongo_doc[target_field] = value_to_set
        elif mapping_rule.get("include_if_null", False):  # Optional: to explicitly include nulls
            mongo_doc[target_field] = None

    # Add primary key if defined and not already set from a source_column that is the PK
    # This is more for auto-generated PKs, usually the PK comes from source_column
    pk_field = None
    for rule in document_structure_config:
        if rule.get("is_primary_key", False):
            pk_field = rule["target_field"]
            break
    if pk_field and pk_field not in mongo_doc and "_id" not in mongo_doc:
        # If primary key is defined but not mapped from source, this is an issue
        # or it implies an auto-generated ID strategy is missing.
        # For MongoDB, if _id is not provided, it auto-generates one.
        # If your pk_field is NOT "_id", you must ensure it's populated.
        logger.debug(
            f"Primary key field '{pk_field}' defined but not found in mapped document for row. MongoDB will generate _id if '{pk_field}' is not '_id'.")

    return mongo_doc if mongo_doc else None  # Return None if doc is empty


def load_to_mongodb(dataframes_dict: dict, target_configs: list, db_manager: DatabaseManager):
    """
    Loads data from specified DataFrames into MongoDB collections.

    Args:
        dataframes_dict (dict): Dictionary of Pandas DataFrames.
        target_configs (list): List of target collection configurations from ETL mapping.
        db_manager (DatabaseManager): Instance of the DatabaseManager.
    """
    if not db_manager or not db_manager.is_connected():
        logger.error("DatabaseManager not available or not connected. Cannot load to MongoDB.")
        return

    for target_conf in target_configs:
        collection_name = target_conf.get('collection_name')
        source_df_alias = target_conf.get('source_dataframe_alias')
        doc_structure_config = target_conf.get('document_structure')
        index_configs = target_conf.get('indexes', [])
        write_mode = target_conf.get('write_mode', 'insert')  # 'insert', 'upsert'
        upsert_key_fields = target_conf.get('upsert_key_fields', [])  # List of fields for upsert query

        if not all([collection_name, source_df_alias, doc_structure_config]):
            logger.error(
                f"Skipping target due to missing 'collection_name', 'source_dataframe_alias', or 'document_structure': {target_conf}")
            continue

        if source_df_alias not in dataframes_dict:
            logger.error(
                f"Source DataFrame '{source_df_alias}' not found for target collection '{collection_name}'. Skipping.")
            continue

        df_to_load = dataframes_dict[source_df_alias]
        if df_to_load.empty:
            logger.info(
                f"Source DataFrame '{source_df_alias}' for collection '{collection_name}' is empty. Nothing to load.")
            continue

        logger.info(
            f"Processing {len(df_to_load)} rows from '{source_df_alias}' for MongoDB collection '{collection_name}' (mode: {write_mode}).")

        documents_for_db = []
        batch_size = target_conf.get('batch_size', 1000)  # Configurable batch size

        for index, row_series in df_to_load.iterrows():
            mongo_doc = map_row_to_document(row_series, doc_structure_config)
            if mongo_doc:  # Only process if mapping resulted in a document
                documents_for_db.append(mongo_doc)

            if len(documents_for_db) >= batch_size:
                if write_mode == 'insert':
                    db_manager.insert_many(collection_name, documents_for_db, ordered=False)
                elif write_mode == 'upsert':
                    if not upsert_key_fields:
                        logger.error(
                            f"Write mode is 'upsert' for '{collection_name}' but 'upsert_key_fields' is not defined. Skipping batch.")
                    else:
                        for doc_to_upsert in documents_for_db:
                            query = {key: doc_to_upsert.get(key) for key in upsert_key_fields if
                                     doc_to_upsert.get(key) is not None}
                            if not query or len(query) != len(
                                    upsert_key_fields):  # Ensure all key fields are present for a valid query
                                logger.warning(
                                    f"Skipping upsert for doc due to missing key fields: {doc_to_upsert}. Query: {query}")
                                # Optionally insert if keys are missing, or log and skip
                                db_manager.insert_one(collection_name,
                                                      doc_to_upsert)  # Fallback to insert? Or handle error.
                                continue
                            update_doc = {"$set": doc_to_upsert}
                            db_manager.update_one(collection_name, query, update_doc, upsert=True)
                logger.debug(f"Processed batch of {len(documents_for_db)} documents for '{collection_name}'.")
                documents_for_db = []

        # Process any remaining documents in the last batch
        if documents_for_db:
            if write_mode == 'insert':
                db_manager.insert_many(collection_name, documents_for_db, ordered=False)
            elif write_mode == 'upsert':
                if not upsert_key_fields:
                    logger.error(
                        f"Write mode is 'upsert' for '{collection_name}' (final batch) but 'upsert_key_fields' not defined. Skipping.")
                else:
                    for doc_to_upsert in documents_for_db:
                        query = {key: doc_to_upsert.get(key) for key in upsert_key_fields if
                                 doc_to_upsert.get(key) is not None}
                        if not query or len(query) != len(upsert_key_fields):
                            logger.warning(
                                f"Skipping upsert for doc due to missing key fields (final batch): {doc_to_upsert}. Query: {query}")
                            db_manager.insert_one(collection_name, doc_to_upsert)
                            continue
                        update_doc = {"$set": doc_to_upsert}
                        db_manager.update_one(collection_name, query, update_doc, upsert=True)
            logger.debug(f"Processed final batch of {len(documents_for_db)} documents for '{collection_name}'.")

        # Create indexes after data loading for this collection
        if index_configs:
            logger.info(f"Creating indexes for collection '{collection_name}'...")
            for index_def in index_configs:
                field_spec = index_def['field']  # Can be string or list of tuples for compound
                unique = index_def.get('unique', False)
                index_name = index_def.get('name')  # Optional custom name
                db_manager.create_index(collection_name, field_spec, unique=unique, index_name=index_name)

        logger.info(f"Finished processing and loading for collection '{collection_name}'.")