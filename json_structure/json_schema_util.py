import json
from pathlib import Path

def get_type_str(value):
    """Maps Python types to simple string representations."""
    if isinstance(value, str):
        return "str"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, list):
        # For lists, you might want to inspect the type of elements if it's homogeneous
        # For now, just "list". You could extend this to be "list[str]", "list[dict]", etc.
        return "list"
    elif isinstance(value, dict):
        return "dict"
    elif value is None:
        return "null"
    else:
        return "unknown"

def generate_structure_from_object(sample_object: dict, file_name: str = "source_data.json", collection_name: str = "data_collection"):
    """
    Generates a JSON structure describing the mapping of a given Python dictionary.

    Args:
        sample_object (dict): The Python dictionary to analyze.
        file_name (str): Placeholder file name for the output structure.
        collection_name (str): Placeholder collection name for the output structure.

    Returns:
        dict: The generated structure as a Python dictionary, or None if sample_object is not a dict.
    """
    if not isinstance(sample_object, dict):
        print("Error: Input must be a dictionary.")
        return None

    mapping = {}
    for key, value in sample_object.items():
        mapping[key] = {
            "field": key, # By default, use the source field name
            "type": get_type_str(value)
        }

    output_structure = {
        "collections": [
            {
                "file": file_name,
                "collection": collection_name,
                "mapping": mapping
            }
        ]
    }
    return output_structure


# The original function for file processing (keeping it for completeness if needed elsewhere)
def generate_structure_json(input_json_path_str: str, collection_name: str = None):
    input_json_path = Path(input_json_path_str)
    if not input_json_path.exists():
        print(f"Error: Input file '{input_json_path}' not found.")
        return None

    data_for_sampling = None
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            try:
                data_for_sampling = json.load(f)
            except json.JSONDecodeError as e1:
                print(f"Info: Standard JSON parsing failed ({e1}). Attempting to read as NDJSON (first line).")
                f.seek(0)
                first_line = f.readline()
                if not first_line:
                    print(f"Error: File '{input_json_path}' is empty or first line is empty after failing standard JSON parse.")
                    return None
                try:
                    sample_object_ndjson = json.loads(first_line)
                    data_for_sampling = [sample_object_ndjson] # Treat as a list with one item
                except json.JSONDecodeError as e2:
                    print(f"Error: Could not decode JSON from '{input_json_path}'. Failed as standard JSON and failed to parse the first line. Error: {e2}")
                    return None
    except Exception as e:
        print(f"Error reading file '{input_json_path}': {e}")
        return None

    sample_object = None
    if isinstance(data_for_sampling, list):
        if data_for_sampling:
            sample_object = data_for_sampling[0]
        else:
            print(f"Warning: Input JSON file '{input_json_path}' (or its first line) is an empty list. Cannot infer structure.")
            return generate_structure_from_object({}, file_name=input_json_path.name, collection_name=collection_name or input_json_path.stem) # Return empty mapping structure
    elif isinstance(data_for_sampling, dict):
        sample_object = data_for_sampling
    else:
        print(f"Warning: Input JSON in '{input_json_path}' is not a list or a dictionary. Cannot infer structure.")
        return None

    if not isinstance(sample_object, dict): # If the first item in a list wasn't a dict
        print(f"Warning: The sample object derived from '{input_json_path}' is not a dictionary. Cannot infer structure.")
        # Create an empty mapping if we have a filename and collection name
        return generate_structure_from_object({}, file_name=input_json_path.name, collection_name=collection_name or input_json_path.stem)


    return generate_structure_from_object(
        sample_object,
        file_name=input_json_path.name,
        collection_name=collection_name or input_json_path.stem
    )