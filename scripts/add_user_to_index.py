# scripts/add_user_to_index.py
import sys
import os
import numpy as np

# Add project root to PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from recommender.user_profile_index import UserProfileIndex
from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager

def main():
    """
    Adds a new user to the FAISS index.
    """
    logger = LoggerManager().get_logger()
    logger.info("Starting add user to FAISS index test...")

    try:
        # Initialize User Profile Index
        path_registry = PathRegistry()
        path_registry.set_path('config_file', os.path.join(project_root, 'config.json'))
        path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')

        index_dir = path_registry.get_path('processed_datasets_dir')
        
        if not index_dir:
            logger.error(f"Could not resolve path for 'processed_datasets_dir'.")
            return
        
        user_index_path = os.path.join(index_dir, 'user_profile_index_test.faiss')
        vector_size = 500
        user_profile_index = UserProfileIndex(vector_size=vector_size, index_path=user_index_path)
        
        # Load the existing index
        user_profile_index.load()
        
        if user_profile_index.index is None:
            logger.error("Failed to load the FAISS index. Please build it first.")
            return

        # Create a sample new user
        new_user_id = "new_user_123"
        new_user_vector = np.random.rand(vector_size).astype(np.float32)
        
        # Get the next available integer ID
        next_int_id = max(user_profile_index.int_to_str_id_map.keys()) + 1
        
        # Add the new user to the index
        user_profile_index.add(next_int_id, new_user_vector)
        
        # Update the ID map
        user_profile_index.int_to_str_id_map[next_int_id] = new_user_id
        user_profile_index.str_to_int_id_map[new_user_id] = next_int_id
        
        # Save the updated index and ID map
        user_profile_index.save()
        
        logger.info(f"Successfully added new user '{new_user_id}' to the FAISS index.")

    except Exception as e:
        logger.critical(f"An error occurred during the test: {e}", exc_info=True)

if __name__ == "__main__":
    main()