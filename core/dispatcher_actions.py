# core/dispatcher_actions.py
import os
import joblib
from typing import Dict, Any, Optional

from etl.loader import exec_all_etl
from recommender.repository import UserInteractionRepository
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.user_profile_repository import UserProfileRepository
from recommender.model import ModelPersister
from etl.MongoDBConnection import MongoDBConnection
from webapp.runner import run_web_ui as web_ui
from core.utils.LoggerManager import LoggerManager
from core.PathRegistry import PathRegistry
from core.utils.dataset_analyzer.schema_generator import process_all_json_in_directory
from core.recommender_factory import initialize_recommender_facade

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

    etl_paths = [os.path.join(etl_configs_dir, etl_file) for etl_file in etl_list]  # type: ignore
    exec_all_etl(etl_paths, app_config, registry)


def load_specific_etl(etl_name: str, app_config: Dict[str, Any], registry: PathRegistry) -> None:
    """Load a specific ETL configuration."""
    logger = logger_manager.get_logger()
    logger.info(f"Loading specific ETL: {etl_name}")

    etl_configs_dir = registry.get_path('etl_configs_dir')
    etl_path = os.path.join(etl_configs_dir, etl_name)  # type: ignore

    if not os.path.exists(etl_path):
        logger.error(f"ETL config not found: {etl_path}")
        return

    exec_all_etl([etl_path], app_config, registry)


# --- Recommendation Actions ---

def recommend_by_titles(titles: list[str], top_n: int = 10) -> None:
    """Generate recommendations based on book titles."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating recommendations for titles: {titles}")

    facade = initialize_recommender_facade()
    if not facade or not facade.content_recommender:
        logger.error("Could not initialize recommender facade for title-based recommendation.")
        return

    try:
        # We use the content_recommender directly from the facade
        recommendations = facade.content_recommender.get_recommendations_by_titles(
            book_titles=titles, top_n=top_n
        )

        print("\n=== Recommendations ===")
        for i, title in enumerate(recommendations, 1):
            print(f"  {i}. {title}")
    except Exception as e:
        logger.error(f"Title-based recommendation failed: {e}", exc_info=True)


def recommend_for_user_id_content_based(user_id: str, top_n: int = 10) -> None:
    """Generate content-based recommendations for a user ID."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating content-based recommendations for user: {user_id}")

    facade = initialize_recommender_facade()
    if not facade:
        return

    recommendations = facade.recommend_with_content_based(user_id, top_n=top_n)
    if recommendations:
        print(f"\n=== Top {top_n} Content-Based Recommendations for User {user_id} ===")
        for i, title in enumerate(recommendations, 1):
            print(f"  {i}. {title}")
    else:
        logger.warning("Could not generate content-based recommendations.")


def recommend_for_user_id_collaborative(user_id: str, top_n: int = 10) -> None:
    """Generate collaborative filtering recommendations for a user ID."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating collaborative filtering recommendations for user: {user_id}")

    facade = initialize_recommender_facade()
    if not facade:
        return

    recommendations = facade.recommend_with_collaborative_filtering(user_id, top_n=top_n)
    if recommendations:
        print(f"\n=== Top {top_n} Collaborative Filtering Recommendations for User {user_id} ===")
        for i, title in enumerate(recommendations, 1):
            print(f"  {i}. {title}")
    else:
        logger.warning("Could not generate collaborative filtering recommendations.")


def recommend_for_user_id(user_id: str, top_n: int = 10) -> None:
    """DEPRECATED: Generate recommendations for a user ID."""
    logger = logger_manager.get_logger()
    logger.warning("This function is deprecated. Use --by-user-id-content-based or --by-user-id-collaborative instead.")


def recommend_from_profile_file(profile_path: str, top_n: int = 10) -> None:
    """Generate recommendations from a profile file."""
    logger = logger_manager.get_logger()
    logger.info(f"Generating recommendations from profile: {profile_path}")
    logger.warning("Function not implemented.")


# --- Web UI Action ---
def run_web_ui(app_config: Dict[str, Any]) -> None:
    """Launch the web interface."""
    logger = logger_manager.get_logger()
    logger.info("Starting web UI")
    web_ui(app_config)


# --- Data Tools Actions ---
def infer_schema(input_dir: str, output_path: Optional[str] = None, output_mode: str = 'both') -> None:
    """Perform schema inference on JSON files."""
    logger = logger_manager.get_logger()
    logger.info(f"Inferring schemas from: {input_dir}")

    try:
        # Ensure that None is handled correctly if passed to the processing function
        # The function should accept Optional[str] or we should provide a default empty path.
        # For now, we assume the function can handle None.
        individual_dir: Optional[str] = None
        aggregate_file: Optional[str] = None

        if output_mode in ['individual', 'both']:
            individual_dir = output_path
        if output_mode in ['aggregate', 'both']:
            aggregate_file = output_path

        process_all_json_in_directory(
            input_dir_path_str=input_dir,
            output_dir_path_str=individual_dir,
            aggregate_output_file_path_str=aggregate_file
        )
    except Exception as e:
        logger.error(f"Schema inference failed: {e}", exc_info=True)


def build_user_profiles() -> None:
    """
    Populates the 'user_profiles' collection and builds the FAISS index.
    """
    logger = logger_manager.get_logger()
    logger.info("--- Starting Full User Profile Generation and Indexing Process ---")

    # STAGE 0: INITIALIZE DEPENDENCIES
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

    # STAGE 1: POPULATE USER PROFILES
    logger.info("[STAGE 1] Starting user profile population...")

    reviews_collection = db_conn.get_database().reviews

    logger.info("Fetching unique user IDs using an aggregation pipeline...")
    pipeline = [{'$group': {'_id': '$user_id'}}]
    cursor = reviews_collection.aggregate(pipeline, allowDiskUse=True)
    all_user_ids = [doc['_id'] for doc in cursor]

    total_users = len(all_user_ids)
    logger.info(f"Found {total_users} unique users in the 'reviews' collection.")

    for i, user_id in enumerate(all_user_ids):
        if user_profile_repo.find_by_user_id(user_id) is not None:
            if (i + 1) % 1000 == 0:
                logger.info(f"Progress: {i + 1}/{total_users}. User '{user_id}' profile already exists. Skipping.")
            continue

        logger.info(f"Progress: {i + 1}/{total_users}. Processing user '{user_id}'...")

        user_history_df = interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            logger.warning(f"No interaction history found for user '{user_id}'. Skipping.")
            continue

        profile_vector = taste_vector_calculator.calculate(user_history_df)

        if profile_vector is not None:
            user_profile_repo.save_or_update(user_id, profile_vector)
            logger.info(f"Successfully created and saved profile for user '{user_id}'.")
        else:
            logger.warning(f"Could not calculate profile for user '{user_id}'.")

    logger.info("[STAGE 1] User profile population complete.")

    # STAGE 2: BUILD AND SAVE FAISS INDEX
    logger.info("[STAGE 2] Starting FAISS index build...")

    all_profiles = user_profile_repo.get_all_profiles_except(user_id_to_exclude=None)
    if not all_profiles:
        logger.critical("No user profiles found in the database after population stage. Cannot build the index.")
        return

    logger.info(f"Successfully fetched {len(all_profiles)} user profiles for indexing.")

    index_dir = path_registry.get_path(recommender_config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        logger.critical(f"Could not resolve path for '{recommender_config.MODEL_ARTIFACTS_DIR_KEY}'. Aborting.")
        return

    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)

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