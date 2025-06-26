# core/dispatcher_actions.py

import argparse
import os
from typing import Dict, Any

from etl.loader import exec_all_etl
from recommender.engine import ContentBasedAnnoyRecommender
from recommender.user_profiler import UserProfiler
from core.utils.logger import LoggerManager
from core.PathRegistry import PathRegistry
from webapp.runner import run_web_ui

logger_manager = LoggerManager()

# --- ETL Actions ---

def load_all_configured_etls(app_config: Dict[str, Any], registry: PathRegistry) -> None:
    """Handles loading all configured ETLs from the application config."""
    logger = logger_manager.get_logger()
    logger.info("Dispatching action: Loading all configured ETLs.")

    etl_list = app_config.get("etl_list", [])
    etl_configs_base_dir = registry.get_path('etl_configs_dir')

    if not etl_list:
        logger.warning("No ETL mapping files listed in 'etl_list' in the application config.")
        return
    if not etl_configs_base_dir:
        logger.error("Path for 'etl_configs_dir' not found in PathRegistry. Cannot locate ETL files.")
        return

    logger.info(f"Found {len(etl_list)} ETL configurations to process: {etl_list}")
    etl_list_paths = [os.path.join(etl_configs_base_dir, etl_file) for etl_file in etl_list]
    exec_all_etl(etl_list_paths, app_config, PathRegistry())

# You can add the specific ETL loader function here if you need it
# def load_specific_etl(etl_name: str, app_config: Dict[str, Any], registry: PathRegistry) -> None:
#     ...

# --- Recommender Actions ---

def get_recommendations(args: argparse.Namespace) -> None:
    """Handles the --recommend flag."""
    logger = logger_manager.get_logger()
    
    input_books = args.recommend
    top_n = args.top_n

    logger.info(f"Attempting to get {top_n} recommendations for: {input_books}")

    try:
        recommender_engine = ContentBasedAnnoyRecommender()
        recommendations = recommender_engine.get_recommendations(input_book_titles=input_books, top_n=top_n)

        if recommendations:
            print("\n--- Top Recommendations ---")
            for i, (title, score) in enumerate(recommendations):
                print(f"{i+1}. {title} (Score: {score:.4f})")
            print("---------------------------\n")
        else:
            logger.warning("Could not generate any recommendations.")
    except Exception as e:
        logger.critical(f"An error occurred during the recommendation process: {e}", exc_info=True)


def recommend_from_profile_file(args: argparse.Namespace) -> None:
    """Handles the --profile_file flag."""
    logger = logger_manager.get_logger()
    profile_file_path = args.profile_file
    top_n = args.top_n
    
    logger.info("Starting recommendation process based on user profile file...")
    
    try:
        recommender = ContentBasedAnnoyRecommender()
        profiler = UserProfiler(recommender)
        profile_data = profiler.create_weighted_profile_from_file(profile_file_path)
        
        if profile_data is None:
            logger.error("Profile creation failed. Aborting.")
            return
            
        user_profile_vector, read_book_indices = profile_data
        recommendations = recommender.get_recommendations_by_profile(
            user_profile_vector,
            read_book_indices,
            top_n=top_n
        )
        
        if recommendations:
            print("\n--- Top Recommendations Based on Your Profile ---")
            for i, (title, score) in enumerate(recommendations):
                print(f"{i+1}. {title} (Score: {score:.4f})")
            print("--------------------------------------------------\n")
        else:
            logger.warning("Could not generate recommendations for the provided profile.")
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}", exc_info=True)


def recommend_for_user_id(args: argparse.Namespace) -> None:
    """Handles the --user_profile flag."""
    logger = logger_manager.get_logger()
    user_id = args.user_profile
    top_n = args.top_n
    
    logger.info(f"Starting recommendation process for user ID: {user_id}")
    
    try:
        recommender = ContentBasedAnnoyRecommender()
        profiler = UserProfiler(recommender)
        profile_data = profiler.create_weighted_profile(user_id)
        
        if profile_data:
            user_profile_vector, read_book_indices = profile_data
            recommendations = recommender.get_recommendations_by_profile(
                user_profile_vector, read_book_indices, top_n=top_n
            )

            if recommendations:
                print(f"\n--- Top {top_n} Recommendations for User {user_id} ---")
                for i, title in enumerate(recommendations):
                    print(f"{i+1}. {title}")
                print("--------------------------------------------------\n")
            else:
                logger.warning("Could not generate recommendations for the provided user.")
        else:
            logger.error(f"Profile creation failed for user {user_id}.")

    except Exception as e:
        logger.critical(f"A critical error occurred while recommending for user {user_id}: {e}", exc_info=True)

# --- Web UI Action ---

def run_web_ui(app_config: Dict[str, Any]) -> None:
    """Handles the --webui flag."""
    logger = logger_manager.get_logger()
    logger.info("Dispatching action: Run Web User Interface.")
    run_web_ui(app_config)