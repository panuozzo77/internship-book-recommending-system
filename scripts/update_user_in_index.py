# scripts/update_user_in_index.py
import sys
import os
import numpy as np

# Add project root to PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.MongoDBConnection import MongoDBConnection
from recommender.repository import UserInteractionRepository
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.user_profile_repository import UserProfileRepository
from recommender.user_profile_index import UserProfileIndex
from core.PathRegistry import PathRegistry
from recommender.model import ModelPersister
from core.utils.LoggerManager import LoggerManager

def main(user_id: str):
    """
    Updates a user's profile in the database and FAISS index.
    """
    logger = LoggerManager().get_logger()
    logger.info(f"Starting profile update for user '{user_id}'...")

    db_conn = None
    try:
        path_registry = PathRegistry()
        path_registry.set_path('config_file', os.path.join(project_root, 'config.json'))
        path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')

        # Initialize MongoDB Connection
        db_conn = MongoDBConnection()
        logger.info("MongoDB connection successful.")

        # Initialize repositories and services
        interaction_repo = UserInteractionRepository(db_conn)
        user_profile_repo = UserProfileRepository(db_conn)
        
        
        persister = ModelPersister(path_registry)
        model = persister.load(version="1.0")
        
        if not model:
            logger.error("Failed to load recommender model. Aborting.")
            return
            
        taste_vector_calculator = TasteVectorCalculator(model)
        
        index_dir = path_registry.get_path('processed_datasets_dir')
        if not index_dir:
            logger.error("Could not resolve path for 'processed_datasets_dir'.")
            return
            
        user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
        vector_size = 500
        user_profile_index = UserProfileIndex(vector_size=vector_size, index_path=user_index_path)
        user_profile_index.load()

        # Calculate new taste vector
        user_history_df = interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            logger.warning(f"No interaction history found for user '{user_id}'. Cannot update profile.")
            return

        profile_vector = taste_vector_calculator.calculate(user_history_df)
        if profile_vector is None:
            logger.error(f"Failed to calculate taste vector for user '{user_id}'.")
            return

        # Save profile to database
        user_profile_repo.save_or_update(user_id, profile_vector)
        logger.info(f"Successfully saved profile for user '{user_id}' to the database.")

        # Update FAISS index
        if user_profile_index.index is None:
            logger.error("FAISS index not loaded. Please build it first.")
            return

        if user_id in user_profile_index.str_to_int_id_map:
            # Existing user: remove old vector and add new one
            int_id = user_profile_index.str_to_int_id_map[user_id]
            user_profile_index.index.remove_ids(np.array([int_id]))
            user_profile_index.add(int_id, profile_vector)
            logger.info(f"Successfully updated vector for existing user '{user_id}' in the FAISS index.")
        else:
            # New user: add to index
            next_int_id = max(user_profile_index.int_to_str_id_map.keys()) + 1
            user_profile_index.add(next_int_id, profile_vector)
            user_profile_index.int_to_str_id_map[next_int_id] = user_id
            user_profile_index.str_to_int_id_map[user_id] = next_int_id
            logger.info(f"Successfully added new user '{user_id}' to the FAISS index.")

        # Save the updated index and ID map
        user_profile_index.save()

    except Exception as e:
        logger.critical(f"An error occurred during the update: {e}", exc_info=True)
    finally:
        if db_conn:
            db_conn.close_connection()
            logger.info("MongoDB connection closed.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/update_user_in_index.py <user_id>")
        sys.exit(1)
    
    main(sys.argv[1])