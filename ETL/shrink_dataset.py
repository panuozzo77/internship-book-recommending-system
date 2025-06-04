import json
import os

def shrink_json_lines_file(input_filepath, output_filepath, num_lines=100):
    """
    Shrinks a JSON Lines file to a specified number of lines.
    Each line in the input file is expected to be a valid JSON object.
    """
    print(f"Shrinking '{input_filepath}' to '{output_filepath}' (first {num_lines} lines)...")
    count = 0
    try:
        with open(input_filepath, 'r', encoding='utf-8') as infile, \
             open(output_filepath, 'w', encoding='utf-8') as outfile:
            for line in infile:
                if count < num_lines:
                    # Ensure the line is valid JSON before writing (optional, but good practice)
                    try:
                        json.loads(line) # Try to parse
                        outfile.write(line)
                        count += 1
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line: {line.strip()}")
                else:
                    break
        print(f"Successfully created '{output_filepath}' with {count} lines.")
    except FileNotFoundError:
        print(f"Error: Input file '{input_filepath}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def shrink_csv_file(input_filepath, output_filepath, num_lines=101): # 100 data lines + header
    """
    Shrinks a CSV file to a specified number of lines (including header).
    """
    print(f"Shrinking '{input_filepath}' to '{output_filepath}' (first {num_lines} lines)...")
    count = 0
    try:
        with open(input_filepath, 'r', encoding='utf-8') as infile, \
             open(output_filepath, 'w', encoding='utf-8') as outfile:
            for line in infile:
                if count < num_lines:
                    outfile.write(line)
                    count += 1
                else:
                    break
        print(f"Successfully created '{output_filepath}' with {count} lines.")
    except FileNotFoundError:
        print(f"Error: Input file '{input_filepath}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # --- Configuration ---
    # Replace with the actual path to one of your large Goodreads files
    # Example: Using goodreads_books.json (which is often JSON Lines)
    # PATH = "./downloaded_datasets/partial/goodreads_books.json.gz" # If gzipped
    ORIGINAL_FOLDER = '/home/cristian/Documents/dataset'
    FILE_NAME = "../downloaded_datasets/partial/goodreads_book_authors.json"  # Assuming unzipped

    PATH = ORIGINAL_FOLDER + '/' + FILE_NAME

    SHRUNKEN_DATA_FILE = FILE_NAME
    LINES_TO_KEEP = 100

    # Determine file type (simplified)
    if PATH.endswith(".json") or PATH.endswith(".jsonl"):
        # You might need to unzip .json.gz first if you don't handle it in the function
        # For .json.gz, you'd use `import gzip` and `gzip.open(...)`
        if PATH.endswith(".gz"):
            print(f"Please unzip '{FILE_NAME}' first, or modify the script to handle .gz files.")
        else:
            shrink_json_lines_file(PATH, SHRUNKEN_DATA_FILE, LINES_TO_KEEP)

    elif PATH.endswith(".csv"):
        if PATH.endswith(".gz"):
            print(f"Please unzip '{FILE_NAME}' first, or modify the script to handle .gz files.")
        else:
            shrink_csv_file(PATH, SHRUNKEN_DATA_FILE, LINES_TO_KEEP +1) # +1 for header
    else:
        print(f"Unsupported file type for shrinking: {FILE_NAME}")

    # --- Example for a CSV if you had one ---
    # ORIGINAL_CSV_FILE = "path/to/your/large.csv"
    # SHRUNKEN_CSV_FILE = "shrunken_data.csv"
    # if os.path.exists(ORIGINAL_CSV_FILE):
    #     shrink_csv_file(ORIGINAL_CSV_FILE, SHRUNKEN_CSV_FILE, 101) # 100 data lines + header