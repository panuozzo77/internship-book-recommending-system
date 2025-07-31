# scripts/test_faiss_index.py
import sys
import os
import numpy as np

# Add project root to PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.MongoDBConnection import MongoDBConnection
from recommender.user_profile_repository import UserProfileRepository
from recommender.user_profile_index import UserProfileIndex
from core.PathRegistry import PathRegistry
from recommender import config as recommender_config
from core.utils.LoggerManager import LoggerManager

def main():
    """
    Tests the FAISS index building process.
    """
    logger = LoggerManager().get_logger()
    logger.info("Starting FAISS index build test...")

    db_conn = None
    try:
        # Initialize User Profile Index
        path_registry = PathRegistry()
        # Correctly resolve the path using the registry
        path_registry.set_path('config_file', os.path.join(project_root, 'config.json'))
        path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')

        index_dir = path_registry.get_path('processed_datasets_dir')
        
        # Initialize MongoDB Connection
        logger.info("MongoDB connection successful.")
        db_conn = MongoDBConnection()

        user_profile_repo = UserProfileRepository(db_conn)
        # Initialize User Profile Repository
        logger.info("UserProfileRepository initialized.")
        
        if not index_dir:
            logger.error(f"Could not resolve path for 'processed_datasets_dir'.")
            return
        
        user_index_path = os.path.join(index_dir, 'user_profile_index_test.faiss')
        vector_size = 500
        user_profile_index = UserProfileIndex(vector_size=vector_size, index_path=user_index_path)
        logger.info("UserProfileIndex initialized.")

        # Fetch all profiles
        logger.info("Fetching all user profiles...")
        all_profiles = user_profile_repo.get_all_profiles_except(user_id_to_exclude=None)
        
        if not all_profiles:
            logger.warning("No user profiles found in the database.")
            return

        logger.info(f"Fetched {len(all_profiles)} profiles.")

        # Check vector dimensions
        invalid_profiles = []
        profiles_for_indexing = []
        
        # Create a mapping from integer index to string user_id
        int_to_str_id_map = {}

        for i, profile in enumerate(all_profiles):
            user_id = profile.get('user_id')
            taste_vector = profile.get('taste_vector')

            if user_id and taste_vector is not None:
                vector = np.array(taste_vector)
                if vector.ndim == 1 and vector.shape[0] == vector_size:
                    profiles_for_indexing.append({'user_id': i, 'taste_vector': vector})
                    int_to_str_id_map[i] = user_id
                else:
                    logger.error(f"Profile for user '{user_id}' has an invalid vector shape: {vector.shape}. Expected ({vector_size},).")
                    invalid_profiles.append(profile)
            else:
                logger.error(f"Profile for user '{user_id}' is missing data or has an incorrect format.")
                invalid_profiles.append(profile)

        if invalid_profiles:
            logger.error(f"Found {len(invalid_profiles)} profiles with invalid vectors. Aborting index build.")
            return

        logger.info("All profile vectors seem to have the correct dimension.")

        # Assign the map to the indexer instance before building
        user_profile_index.int_to_str_id_map = int_to_str_id_map

        # Build the index
        user_profile_index.build(profiles_for_indexing)
        user_profile_index.save()
        logger.info("FAISS index built and saved successfully.")

    except Exception as e:
        logger.critical(f"An error occurred during the test: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close_connection()
            logger.info("MongoDB connection closed.")

if __name__ == "__main__":
    main()