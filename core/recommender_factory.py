# core/recommender_factory.py
import os
from typing import Optional
from recommender.facade import UserRecommenderFacade
from recommender.model import ModelPersister
from recommender.repository import UserInteractionRepository
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.user_profile_index import UserProfileIndex
from recommender.user_profile_repository import UserProfileRepository
from recommender.engine import ContentBasedRecommender, CollaborativeFilteringRecommender, PageCountReRanker, GenrePreferenceReRanker
from recommender import config as recommender_config
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager

logger_manager = LoggerManager()

def initialize_recommender_facade() -> Optional[UserRecommenderFacade]:
    """Initializes and returns the fully configured UserRecommenderFacade."""
    logger = logger_manager.get_logger()
    logger.info("Initializing recommendation components...")

    path_registry = PathRegistry()
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)

    model = persister.load(version="1.0")
    if not model:
        logger.critical("Book model could not be loaded. Please run the model build script first.")
        return None

    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)

    index_dir = path_registry.get_path(recommender_config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        logger.critical(f"Could not resolve path for '{recommender_config.MODEL_ARTIFACTS_DIR_KEY}'. Aborting.")
        return None
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)

    rerankers = [GenrePreferenceReRanker(), PageCountReRanker()]
    content_recommender = ContentBasedRecommender(model, rerankers=rerankers)
    collaborative_recommender = CollaborativeFilteringRecommender(model, user_profile_index, rerankers=rerankers)
    taste_vector_calculator = TasteVectorCalculator(model)

    recommender_facade = UserRecommenderFacade(
        content_recommender=content_recommender,
        collaborative_recommender=collaborative_recommender,
        interaction_repo=interaction_repo,
        user_profile_repo=user_profile_repo,
        taste_vector_calculator=taste_vector_calculator,
        user_profile_index=user_profile_index
    )
    recommender_facade.load_indices()
    logger.info("Recommendation components initialized successfully.")
    return recommender_facade