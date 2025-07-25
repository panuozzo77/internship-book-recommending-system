# scripts/check_user_in_index.py
import sys
import os

# Add project root to PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from recommender.user_profile_index import UserProfileIndex
from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager

def main():
    """
    Checks if a user is in the FAISS index.
    """
    logger = LoggerManager().get_logger()
    logger.info("Starting check user in FAISS index test...")

    try:
        # Initialize User Profile Index
        path_registry = PathRegistry()
        path_registry.set_path('config_file', os.path.join(project_root, 'config.json'))
        path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')

        index_dir = path_registry.get_path('processed_datasets_dir')
        
        if not index_dir:
            logger.error(f"Could not resolve path for 'processed_datasets_dir'.")
            return
        
        user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
        vector_size = 500
        user_profile_index = UserProfileIndex(vector_size=vector_size, index_path=user_index_path)
        
        # Load the existing index
        user_profile_index.load()
        
        if user_profile_index.index is None:
            logger.error("Failed to load the FAISS index. Please build it first.")
            return

        # User to check
        user_id_string = 'cristian'

        faiss_id = user_profile_index.str_to_int_id_map.get(user_id_string)

        if faiss_id is not None:
            print(f"L'ID FAISS per l'utente '{user_id_string}' è: {faiss_id}")
        else:
            print(f"L'utente '{user_id_string}' non è stato trovato nella mappa FAISS.")


    except Exception as e:
        logger.critical(f"An error occurred during the test: {e}", exc_info=True)

if __name__ == "__main__":
    main()