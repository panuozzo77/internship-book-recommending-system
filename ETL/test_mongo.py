from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

try:
    # Default connection URI for a local MongoDB instance
    client = MongoClient('mongodb://localhost:27017/')

    # The is_mongos property will trigger a server round-trip
    # and raise ConnectionFailure if the server is not available.
    client.admin.command('ismaster') # or 'ping' for newer versions
    print("Successfully connected to MongoDB!")

    # List databases
    db_list = client.list_database_names()
    print("Databases:", db_list)

    # Select your database (it will be created if it doesn't exist upon first write)
    db = client['goodreads_project_db']
    print(f"Using database: {db.name}")

    # Select a collection (like a table in SQL)
    books_collection = db['books']
    print(f"Using collection: {books_collection.name}")

    # Example: Insert a document
    sample_book = {
        "book_id": "12345_test",
        "title": "A Test Book for MongoDB",
        "authors": [{"name": "Test Author"}],
        "year": 2024,
        "tags": ["python", "mongodb", "test"]
    }
    insert_result = books_collection.insert_one(sample_book)
    print(f"Inserted document ID: {insert_result.inserted_id}")

    # Example: Find a document
    found_book = books_collection.find_one({"book_id": "12345_test"})
    if found_book:
        print("Found book:", found_book)
    else:
        print("Book not found.")

    # Clean up the test document (optional)
    # books_collection.delete_one({"book_id": "12345_test"})
    # print("Test document deleted.")


except ConnectionFailure:
    print("MongoDB connection failed.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'client' in locals() and client:
        client.close()
        print("MongoDB connection closed.")