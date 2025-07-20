# scripts/build_user_profile_index.py
import sys
import os
import joblib
import numpy as np

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from recommender.user_profile_repository import UserProfileRepository
from recommender.user_profile_index import UserProfileIndex
from recommender.model import ModelPersister
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.repository import UserInteractionRepository
from recommender import config
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager

def populate_profiles_and_build_index():
    """
    A two-stage script:
    1. Populates the 'user_profiles' collection by calculating the taste vector for every user.
    2. Builds a FAISS index from the populated profiles and saves it to disk.
    """
    logger = LoggerManager().get_logger()
    logger.info("--- Starting Full User Profile Generation and Indexing Process ---")

    # --- STAGE 0: INITIALIZE DEPENDENCIES ---
    logger.info("[STAGE 0] Initializing dependencies...")
    path_registry = PathRegistry()
    db_conn = MongoDBConnection()
    
    persister = ModelPersister(path_registry)
    model = persister.load(version="1.0")
    if not model:
        logger.critical("Could not load recommender model. Aborting.")
        return
    
    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)
    taste_vector_calculator = TasteVectorCalculator(model)
    logger.info("Dependencies initialized.")

    # --- STAGE 1: POPULATE USER PROFILES ---
    logger.info("[STAGE 1] Starting user profile population...")
    
    reviews_collection = db_conn.get_database().reviews
    
    # Use an aggregation pipeline to get distinct user_ids to avoid the 16MB BSON limit
    logger.info("Fetching unique user IDs using an aggregation pipeline...")
    pipeline = [{'$group': {'_id': '$user_id'}}]
    cursor = reviews_collection.aggregate(pipeline, allowDiskUse=True)
    all_user_ids = [doc['_id'] for doc in cursor]
    
    total_users = len(all_user_ids)
    logger.info(f"Found {total_users} unique users in the 'reviews' collection.")

    for i, user_id in enumerate(all_user_ids):
        # This makes the script resumable
        if user_profile_repo.find_by_user_id(user_id) is not None:
            if (i + 1) % 1000 == 0: # Log progress even for skipped users
                 logger.info(f"Progress: {i+1}/{total_users}. User '{user_id}' profile already exists. Skipping.")
            continue

        logger.info(f"Progress: {i+1}/{total_users}. Processing user '{user_id}'...")
        
        user_history_df = interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            logger.warning(f"No interaction history found for user '{user_id}'. Skipping.")
            continue
            
        # Calculate the taste vector
        profile_vector = taste_vector_calculator.calculate(user_history_df)
        
        # Save the new profile to the database
        if profile_vector is not None:
            user_profile_repo.save_or_update(user_id, profile_vector)
            logger.info(f"Successfully created and saved profile for user '{user_id}'.")
        else:
            logger.warning(f"Could not calculate profile for user '{user_id}'.")

    logger.info("[STAGE 1] User profile population complete.")

    # --- STAGE 2: BUILD AND SAVE FAISS INDEX ---
    logger.info("[STAGE 2] Starting FAISS index build...")
    
    all_profiles = user_profile_repo.get_all_profiles_except(user_id_to_exclude=None)
    if not all_profiles:
        logger.critical("No user profiles found in the database after population stage. Cannot build the index.")
        return
    
    logger.info(f"Successfully fetched {len(all_profiles)} user profiles for indexing.")

    index_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        logger.critical(f"Could not resolve path for '{config.MODEL_ARTIFACTS_DIR_KEY}'. Aborting.")
        return
        
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)

    # Create the integer ID to string ID mapping
    int_id_to_str_id_map = {i: profile['user_id'] for i, profile in enumerate(all_profiles)}
    
    profiles_for_indexing = [
        {'user_id': i, 'taste_vector': profile['taste_vector']}
        for i, profile in enumerate(all_profiles)
    ]

    user_profile_index.build(profiles_for_indexing)
    user_profile_index.save()
    
    map_path = user_index_path.replace('.faiss', '_id_map.joblib')
    joblib.dump(int_id_to_str_id_map, map_path)
    logger.info(f"User ID map saved successfully to {map_path}")
    
    logger.info("--- Full Process Complete ---")


if __name__ == "__main__":
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    PathRegistry().set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    
    populate_profiles_and_build_index()