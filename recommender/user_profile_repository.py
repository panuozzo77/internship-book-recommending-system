# recommender/user_profile_repository.py
from typing import Any, Optional, List, Dict
import numpy as np
from pymongo.results import UpdateResult
from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager

class UserProfileRepository:
    """
    Manages the persistence of user taste vectors in MongoDB.
    This repository handles CRUD operations for user profiles in the 'user_profiles' collection.
    """
    def __init__(self, db_connection: MongoDBConnection, collection_name: str = 'user_profiles'):
        """
        Initializes the repository.

        Args:
            db_connection: An active connection to MongoDB.
            collection_name: The name of the collection to store user profiles.
        """
        self.db = db_connection.get_database()
        self.collection = self.db[collection_name]
        self.logger = LoggerManager().get_logger()
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Ensures that the necessary index on user_id exists for efficient lookups."""
        try:
            self.collection.create_index('user_id', unique=True)
            self.logger.info(f"Index on 'user_id' ensured for '{self.collection.name}' collection.")
        except Exception as e:
            self.logger.error(f"Error creating index on 'user_profiles': {e}", exc_info=True)

    def save_or_update(self, user_id: Any, taste_vector: np.ndarray) -> UpdateResult:
        """
        Saves a new user profile or updates an existing one.

        Args:
            user_id: The unique identifier for the user.
            taste_vector: The user's taste vector as a NumPy array.

        Returns:
            The result of the update operation from MongoDB.
        """
        self.logger.debug(f"Saving profile for user_id '{user_id}'...")
        # MongoDB cannot store NumPy arrays directly, so we convert it to a list of floats.
        vector_as_list = taste_vector.tolist()
        
        query = {'user_id': user_id}
        update = {
            '$set': {
                'taste_vector': vector_as_list,
                'user_id': user_id
            }
        }
        
        # The 'upsert=True' option creates a new document if one doesn't exist.
        result = self.collection.update_one(query, update, upsert=True)
        
        if result.upserted_id:
            self.logger.info(f"New profile created for user_id '{user_id}'.")
        elif result.modified_count > 0:
            self.logger.info(f"Profile updated for user_id '{user_id}'.")
            
        return result

    def find_by_user_id(self, user_id: Any) -> Optional[np.ndarray]:
        """
        Finds a user's profile by their ID and returns their taste vector.

        Args:
            user_id: The user's unique identifier.

        Returns:
            The taste vector as a NumPy array, or None if not found.
        """
        self.logger.debug(f"Fetching profile for user_id '{user_id}'...")
        document = self.collection.find_one({'user_id': user_id})
        
        if document and 'taste_vector' in document:
            # Convert the list back to a NumPy array.
            return np.array(document['taste_vector'], dtype=np.float32)
        
        self.logger.warning(f"Profile not found for user_id '{user_id}'.")
        return None

    def get_all_profiles_except(self, user_id_to_exclude: Any) -> List[Dict[str, Any]]:
        """
        Retrieves all user profiles from the collection except for the specified user.

        This is crucial for the collaborative filtering step, where we need to compare
        the target user's profile against all other users.

        Args:
            user_id_to_exclude: The ID of the user to exclude from the results.

        Returns:
            A list of dictionaries, where each dictionary contains the 'user_id'
            and 'taste_vector' (as a NumPy array) of a user.
        """
        self.logger.debug(f"Fetching all profiles, excluding user_id '{user_id_to_exclude}'...")
        query = {'user_id': {'$ne': user_id_to_exclude}}
        cursor = self.collection.find(query)
        
        profiles = []
        for doc in cursor:
            if 'taste_vector' in doc and 'user_id' in doc:
                profiles.append({
                    'user_id': doc['user_id'],
                    'taste_vector': np.array(doc['taste_vector'], dtype=np.float32)
                })
        
        self.logger.info(f"Fetched {len(profiles)} profiles for neighbor search.")
        return profiles