import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoDBConnection:
    _instance = None
    _client = None
    _db = None

    def __new__(cls, config_file_path='config.json'):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            cls._instance._initialize_connection(config_file_path)
        return cls._instance

    def _initialize_connection(self, config_file_path):
        try:
            with open(config_file_path, 'r') as f:
                config = json.load(f)

            db_config = config['database']
            mongo_uri = db_config['uri']
            db_name = db_config['db_name']

            if not db_name:
                raise ValueError("Database name (db_name_override) is not specified in the config file.")

            self.__class__._client = MongoClient(mongo_uri)
            # Optional: Ping to verify connection immediately
            self.__class__._client.admin.command('ismaster')
            print("Successfully connected to MongoDB!")
            self.__class__._db = self.__class__._client[db_name]
            print(f"Using database: {self.__class__._db.name}")

        except FileNotFoundError:
            print(f"Error: Config file '{config_file_path}' not found.")
            self.__class__._instance = None # Reset instance on failure
            raise
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in '{config_file_path}'.")
            self.__class__._instance = None
            raise
        except ConnectionFailure:
            print("MongoDB connection failed.")
            self.__class__._instance = None
            raise
        except ValueError as ve:
            print(f"Configuration Error: {ve}")
            self.__class__._instance = None
            raise
        except Exception as e:
            print(f"An unexpected error occurred during connection: {e}")
            self.__class__._instance = None
            raise

    def get_client(self):
        if self._client is None:
            raise ConnectionError("MongoDB client not initialized. Call MongoDBConnection() first.")
        return self._client

    def get_database(self):
        if self._db is None:
            raise ConnectionError("MongoDB database not initialized. Call MongoDBConnection() first.")
        return self._db

    def close_connection(self):
        if self._client:
            self._client.close()
            print("MongoDB connection closed.")
            self.__class__._client = None # Clear the client
            self.__class__._db = None    # Clear the db
            self.__class__._instance = None # Clear the instance