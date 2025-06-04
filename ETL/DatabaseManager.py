from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, PyMongoError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


class DatabaseManager:
    """
    A class to manage interactions with a MongoDB database.
    """

    def __init__(self, db_uri="mongodb://localhost:27017/", db_name="goodreads_project_db"):
        """
        Initializes the DatabaseManager.

        Args:
            db_uri (str): The MongoDB connection URI.
            db_name (str): The name of the database to use.
        """
        self.db_uri = db_uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self._connect()

    def _connect(self):
        """Establishes a connection to the MongoDB server."""
        if self.client is None:
            try:
                self.client = MongoClient(self.db_uri, serverSelectionTimeoutMS=5000)  # Timeout for connection
                # The ismaster command is cheap and does not require auth.
                self.client.admin.command('ismaster')
                self.db = self.client[self.db_name]
                logging.info(f"Successfully connected to MongoDB at {self.db_uri} and using database '{self.db_name}'.")
            except ConnectionFailure as e:
                logging.error(f"Failed to connect to MongoDB at {self.db_uri}: {e}")
                self.client = None  # Ensure client is None if connection failed
                self.db = None
                raise  # Re-raise the exception so the caller knows connection failed
            except PyMongoError as e:
                logging.error(f"A PyMongo error occurred during connection: {e}")
                self.client = None
                self.db = None
                raise

    def is_connected(self):
        """Checks if the client is connected to MongoDB."""
        if self.client:
            try:
                # The ismaster command is cheap and does not require auth.
                self.client.admin.command('ismaster')
                return True
            except ConnectionFailure:
                return False
        return False

    def close_connection(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            logging.info("MongoDB connection closed.")

    def get_collection(self, collection_name):
        """
        Gets a specific collection from the database.
        Reconnects if the connection was lost.

        Args:
            collection_name (str): The name of the collection.

        Returns:
            pymongo.collection.Collection: The collection object, or None if connection fails.
        """
        if not self.is_connected():
            logging.warning("Connection lost. Attempting to reconnect...")
            try:
                self._connect()  # Try to reconnect
            except PyMongoError:
                logging.error("Failed to reconnect. Cannot get collection.")
                return None  # Explicitly return None if reconnect fails

        if self.db:  # Check if db is available after potential reconnect
            return self.db[collection_name]
        else:
            logging.error("Database not available. Cannot get collection.")
            return None

    # --- Write Operations ---

    def insert_one(self, collection_name, document):
        """
        Inserts a single document into the specified collection.

        Args:
            collection_name (str): The name of the collection.
            document (dict): The document to insert.

        Returns:
            pymongo.results.InsertOneResult or None: The result of the insert operation, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.insert_one(document)
            logging.debug(f"Inserted document with ID {result.inserted_id} into '{collection_name}'.")
            return result
        except OperationFailure as e:
            logging.error(f"Error inserting document into '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during insert_one into '{collection_name}': {e}")
        return None

    def insert_many(self, collection_name, documents, ordered=True):
        """
        Inserts multiple documents into the specified collection.

        Args:
            collection_name (str): The name of the collection.
            documents (list of dict): A list of documents to insert.
            ordered (bool): If True, perform an ordered insert (stops on first error).
                            If False, perform an unordered insert (continues on errors).

        Returns:
            pymongo.results.InsertManyResult or None: The result of the insert operation, or None on failure.
        """
        if not documents:
            logging.warning(f"No documents provided for insert_many into '{collection_name}'.")
            return None

        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.insert_many(documents, ordered=ordered)
            logging.debug(f"Inserted {len(result.inserted_ids)} documents into '{collection_name}'.")
            return result
        except OperationFailure as e:  # Specific errors like BulkWriteError
            logging.error(f"Error inserting documents into '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during insert_many into '{collection_name}': {e}")
        return None

    # --- Read Operations ---

    def find_one(self, collection_name, query=None, projection=None):
        """
        Finds a single document in the specified collection.

        Args:
            collection_name (str): The name of the collection.
            query (dict, optional): The query criteria. Defaults to None (matches any).
            projection (dict, optional): Specifies which fields to include or exclude.
                                        Example: {"name": 1, "email": 1, "_id": 0}

        Returns:
            dict or None: The document found, or None if no document matches or on error.
        """
        query = query or {}
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            document = collection.find_one(query, projection)
            return document
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during find_one from '{collection_name}': {e}")
        return None

    def find_many(self, collection_name, query=None, projection=None, limit=0, sort=None):
        """
        Finds multiple documents in the specified collection.

        Args:
            collection_name (str): The name of the collection.
            query (dict, optional): The query criteria. Defaults to None (matches all).
            projection (dict, optional): Specifies which fields to include or exclude.
            limit (int, optional): The maximum number of documents to return. 0 means no limit.
            sort (list of tuples, optional): A list of (key, direction) pairs to sort by.
                                            Example: [("name", 1), ("age", -1)] (1 for asc, -1 for desc)

        Returns:
            pymongo.cursor.Cursor or None: A cursor object for the results, or None on error.
                                           The caller is responsible for iterating over the cursor.
        """
        query = query or {}
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            cursor = collection.find(query, projection)
            if sort:
                cursor = cursor.sort(sort)
            if limit > 0:
                cursor = cursor.limit(limit)
            return cursor
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during find_many from '{collection_name}': {e}")
        return None

    def count_documents(self, collection_name, query=None):
        """
        Counts the number of documents matching the query in the specified collection.

        Args:
            collection_name (str): The name of the collection.
            query (dict, optional): The query criteria. Defaults to None (counts all).

        Returns:
            int: The number of documents, or -1 on error.
        """
        query = query or {}
        collection = self.get_collection(collection_name)
        if collection is None: return -1
        try:
            count = collection.count_documents(query)
            return count
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during count_documents for '{collection_name}': {e}")
        return -1

    # --- Update Operations ---

    def update_one(self, collection_name, query, update_document, upsert=False):
        """
        Updates a single document matching the query.

        Args:
            collection_name (str): The name of the collection.
            query (dict): The selection criteria for the update.
            update_document (dict): The modifications to apply (e.g., using $set, $inc).
            upsert (bool): If True, perform an upsert (insert if no document matches).

        Returns:
            pymongo.results.UpdateResult or None: The result of the update operation, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.update_one(query, update_document, upsert=upsert)
            logging.debug(
                f"Update one in '{collection_name}': Matched {result.matched_count}, Modified {result.modified_count}, Upserted ID {result.upserted_id}")
            return result
        except OperationFailure as e:
            logging.error(f"Error updating one document in '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during update_one in '{collection_name}': {e}")
        return None

    def update_many(self, collection_name, query, update_document, upsert=False):
        """
        Updates multiple documents matching the query.

        Args:
            collection_name (str): The name of the collection.
            query (dict): The selection criteria for the update.
            update_document (dict): The modifications to apply.
            upsert (bool): If True, perform an upsert (insert if no document matches - less common for update_many).

        Returns:
            pymongo.results.UpdateResult or None: The result of the update operation, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.update_many(query, update_document, upsert=upsert)
            logging.debug(
                f"Update many in '{collection_name}': Matched {result.matched_count}, Modified {result.modified_count}, Upserted ID {result.upserted_id}")
            return result
        except OperationFailure as e:
            logging.error(f"Error updating many documents in '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during update_many in '{collection_name}': {e}")
        return None

    # --- Delete Operations ---

    def delete_one(self, collection_name, query):
        """
        Deletes a single document matching the query.

        Args:
            collection_name (str): The name of the collection.
            query (dict): The selection criteria for the deletion.

        Returns:
            pymongo.results.DeleteResult or None: The result of the delete operation, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.delete_one(query)
            logging.debug(f"Delete one from '{collection_name}': Deleted {result.deleted_count} document(s).")
            return result
        except OperationFailure as e:
            logging.error(f"Error deleting one document from '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during delete_one from '{collection_name}': {e}")
        return None

    def delete_many(self, collection_name, query):
        """
        Deletes multiple documents matching the query.

        Args:
            collection_name (str): The name of the collection.
            query (dict): The selection criteria for the deletion.

        Returns:
            pymongo.results.DeleteResult or None: The result of the delete operation, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            result = collection.delete_many(query)
            logging.debug(f"Delete many from '{collection_name}': Deleted {result.deleted_count} document(s).")
            return result
        except OperationFailure as e:
            logging.error(f"Error deleting many documents from '{collection_name}': {e.details}")
        except PyMongoError as e:
            logging.error(f"A PyMongo error occurred during delete_many from '{collection_name}': {e}")
        return None

    # --- Helper/Management ---
    def create_index(self, collection_name, field_name, unique=False, index_name=None):
        """
        Creates an index on a specified field in a collection.

        Args:
            collection_name (str): The name of the collection.
            field_name (str or list of tuples): The field to index (e.g., "book_id")
                                               or a list for compound indexes (e.g., [("author_id", 1), ("title", 1)]).
            unique (bool): If True, creates a unique index.
            index_name (str, optional): A custom name for the index.

        Returns:
            str or None: The name of the created index, or None on failure.
        """
        collection = self.get_collection(collection_name)
        if collection is None: return None
        try:
            if index_name:
                idx_name = collection.create_index([(field_name, 1)] if isinstance(field_name, str) else field_name,
                                                   unique=unique, name=index_name)
            else:
                idx_name = collection.create_index([(field_name, 1)] if isinstance(field_name, str) else field_name,
                                                   unique=unique)
            logging.info(
                f"Index '{idx_name}' created (or already exists) on '{collection_name}' for field(s) '{field_name}'.")
            return idx_name
        except PyMongoError as e:
            logging.error(f"Error creating index on '{collection_name}' for field '{field_name}': {e}")
        return None

    def drop_collection(self, collection_name):
        """
        Drops (deletes) an entire collection. Use with caution!

        Args:
            collection_name (str): The name of the collection to drop.
        """
        if self.db:
            try:
                self.db.drop_collection(collection_name)
                logging.info(f"Collection '{collection_name}' dropped successfully.")
            except PyMongoError as e:
                logging.error(f"Error dropping collection '{collection_name}': {e}")
        else:
            logging.error("Database not available. Cannot drop collection.")

    def __enter__(self):
        """Support for 'with' statement."""
        # Connection is established in __init__ or get_collection
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for 'with' statement, ensures connection is closed."""
        self.close_connection()
