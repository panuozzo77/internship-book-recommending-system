# core/dispatcher_actions.py
import os
from typing import Dict, Any
from pathlib import Path

from etl.loader import exec_all_etl
from recommender.engine2 import ContentBasedAnnoyRecommender
from recommender.user_profiler import UserProfiler
from webapp.runner import run_web_ui as web_ui
from core.utils.LoggerManager import LoggerManager
from core.PathRegistry import PathRegistry
from core.utils.dataset_analyzer.schema_generator import process_all_json_in_directory

logger_manager = LoggerManager()

# --- ETL Actions ---
def load_all_configured_etls(app_config: Dict[str, Any], registry: PathRegistry) -> None:
    """Load all ETL processes defined in configuration."""
    logger = logger_manager.get_logger()
    logger.info("Loading all configured ETL processes")
    
    etl_list = app_config.get("etl_list", [])
    etl_configs_dir = registry.get_path('etl_configs_dir')

    if not etl_list:
        logger.warning("No ETL configurations found in app_config")
        return

    etl_paths = [os.path.join(etl_configs_dir, etl_file) for etl_file in etl_list] # type: ignore
    exec_all_etl(etl_paths, app_config, registry)

def load_specific_etl(etl_name: str, app_config: Dict[str, Any], registry: PathRegistry) -> None:
    """Load a specific ETL configuration."""
    logger = logger_manager.get_logger()
    logger.info(f"Loading specific ETL: {etl_name}")
    
    etl_configs_dir = registry.get_path('etl_configs_dir')
    etl_path = os.path.join(etl_configs_dir, etl_name) # type: ignore
    
    if not os.path.exists(etl_path):
        logger.error(f"ETL config not found: {etl_path}")
        return

    exec_all_etl([etl_path], app_config, registry)

# --- Recommendation Actions ---
''''''
def recommend_by_titles(titles: list[str], top_n: int = 10) -> None:
    """Generate recommendations based on book titles."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating recommendations for titles: {titles}")
    
    try:
        recommender = ContentBasedAnnoyRecommender()
        recommendations = recommender.get_recommendations(input_book_titles=titles, top_n=top_n)
        
        print("\n=== Recommendations ===")
        for i, (title, score) in enumerate(recommendations):
            print(f"{i+1}. {title} (score: {score:.2f})")
    except Exception as e:
        logger.error(f"Recommendation failed: {e}", exc_info=True)

def recommend_for_user_id(user_id: str, top_n: int = 10) -> None:
    """Generate recommendations for a user ID."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating recommendations for user: {user_id}")
    '''
    try:
        recommender = ContentBasedAnnoyRecommender()
        profiler = UserProfiler(recommender)
        profile_data = profiler.create_weighted_profile(user_id)
        #logger.warning(f"Profile data for user {user_id}: {profile_data}")

        if profile_data:
            user_profile_vector, read_book_indices = profile_data
            recommendations = recommender.get_recommendations_by_profile(
                user_profile_vector,
                read_book_indices,
                top_n=top_n
            )

            print(f"\n=== Recommendations for User {user_id} ===")
            for i, title in enumerate(recommendations):
                print(f"{i+1}. {title}")
        else:
            logger.error("Failed to create user profile")
    except Exception as e:
        logger.error(f"User recommendation failed: {e}", exc_info=True)
        '''

def recommend_from_profile_file(profile_path: str, top_n: int = 10) -> None:
    """Generate recommendations from a profile file."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating recommendations from profile: {profile_path}")
    
    '''
    try:
        recommender = ContentBasedAnnoyRecommender()
        profiler = UserProfiler(recommender)
        profile_data = profiler.create_weighted_profile_from_file(profile_path)
        
        if profile_data:
            recommendations = recommender.get_recommendations_by_profile(
                *profile_data, top_n=top_n
            )
            
            print("\n=== Recommendations from Profile ===")
            for i, (title, score) in enumerate(recommendations):
                print(f"{i+1}. {title} (score: {score:.2f})")
        else:
            logger.error("Failed to create profile from file")
    except Exception as e:
        logger.error(f"Profile recommendation failed: {e}", exc_info=True)
    '''

# --- Web UI Action ---
def run_web_ui(app_config: Dict[str, Any]) -> None:
    """Launch the web interface."""
    logger = logger_manager.get_logger()
    logger.info("Starting web UI")
    web_ui(app_config)

# --- Data Tools Actions ---
def infer_schema(input_dir: str, output_path: str = None, output_mode: str = 'both') -> None:
    """Perform schema inference on JSON files."""
    logger = logger_manager.get_logger()
    logger.info(f"Inferring schemas from: {input_dir}")
    
    try:
        individual_dir = output_path if output_mode in ['individual', 'both'] else None
        aggregate_file = output_path if output_mode in ['aggregate', 'both'] else None
        
        process_all_json_in_directory(
            input_dir_path_str=input_dir,
            output_dir_path_str=individual_dir,
            aggregate_output_file_path_str=aggregate_file
        )
    except Exception as e:
        logger.error(f"Schema inference failed: {e}", exc_info=True)