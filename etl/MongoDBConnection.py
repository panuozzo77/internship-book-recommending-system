#/etl/MongoDBConnection.py
import json
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager

logger = LoggerManager().get_logger()

class MongoDBConnection:
    _instance = None
    _client = None
    _db = None

    def __new__(cls, main_app_config_path: Optional[str] = None) -> 'MongoDBConnection':
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            # If a path is provided, use it. Otherwise, get it from PathRegistry.
            config_path_to_use = main_app_config_path
            if config_path_to_use is None:
                registry = PathRegistry()  # Get singleton instance
                config_path_to_use = registry.get_path('config_file')  # Fetch the registered path
                if config_path_to_use is None:
                    logger.critical(
                        "MongoDBConnection: 'config_file' path not found in PathRegistry and no path provided.")
                    # cls._instance = None # Prevent partial initialization
                    raise ValueError(
                        "MongoDBConnection requires 'config_file' to be set in PathRegistry or a path provided to constructor.")

            cls._instance._initialize_connection(config_path_to_use)
        return cls._instance

    def _initialize_connection(self, main_app_config_path: str):
        # This method remains largely the same as in the previous good version,
        # using main_app_config_path to open and read the main config.json
        logger.debug(f"MongoDBConnection initializing with app config: {main_app_config_path}")
        try:
            with open(main_app_config_path, 'r', encoding='utf-8') as f:
                app_config = json.load(f)

            db_settings = app_config.get('database', {})
            mongo_uri = db_settings.get('uri')
            db_name_for_connection = db_settings.get('db_name') # From main config.json
            username = db_settings.get('username')
            password = db_settings.get('password')

            client_args = {
                "serverSelectionTimeoutMS": 5000
            }

            if not mongo_uri: raise ValueError("MongoDB URI not in app config's database section.")
            if not db_name_for_connection: raise ValueError("Database name ('db_name') not in app config's database section.")
            
            if username and password:
                client_args['username'] = username
                client_args['password'] = password
                logger.info("Attempting to connect to MongoDB with username/password authentication.")
            else:
                logger.info("Attempting to connect to MongoDB without authentication.")

            self.__class__._client = MongoClient(mongo_uri, **client_args)

            

            self.__class__._client.admin.command('ismaster')
            self.__class__._db = self.__class__._client[db_name_for_connection]
            logger.info(f"MongoDBConnection successfully connected to default DB: {self.__class__._db.name} specified in {main_app_config_path}")

        except FileNotFoundError:
            logger.error(f"Error: Main app config file '{main_app_config_path}' not found for MongoDBConnection.")
            # self.__class__._instance = None # Don't nullify instance, just connection state
            self.__class__._client = None
            self.__class__._db = None
            raise
        except json.JSONDecodeError:
            logger.error(f"Error: Invalid JSON in main app config '{main_app_config_path}'.")
            self.__class__._client = None
            self.__class__._db = None
            raise
        except ConnectionFailure as e:
            logger.critical(f"MongoDB connection failed using config {main_app_config_path}: {e}")
            self.__class__._client = None
            self.__class__._db = None
            raise
        except ValueError as ve: # Catch config value errors
            logger.critical(f"MongoDB Configuration Error from {main_app_config_path}: {ve}")
            self.__class__._client = None
            self.__class__._db = None
            raise
        except Exception as e:
            logger.critical(f"An unexpected error occurred during MongoDB connection using {main_app_config_path}: {e}", exc_info=True)
            self.__class__._client = None
            self.__class__._db = None
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