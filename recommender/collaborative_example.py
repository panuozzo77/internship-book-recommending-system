# recommender/collaborative_example.py
import os
from recommender.repository import UserInteractionRepository
from recommender.user_profile_repository import UserProfileRepository
from recommender.facade import UserRecommenderFacade
from recommender.model import ModelPersister
from recommender.engine import (
    ContentBasedRecommender, 
    CollaborativeFilteringRecommender,
    PageCountReRanker, 
    GenrePreferenceReRanker
)
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.user_profile_index import UserProfileIndex
from recommender import config
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry

def run_full_recommendation_showcase(user_id: str):
    """
    A complete script to demonstrate generating recommendations for a user
    using both Content-Based and the scalable Collaborative Filtering strategies.
    """
    print("--- Scalable Recommendation Showcase ---")
    
    # 1. Initialize Core Dependencies
    print("\n[PHASE 1] Initializing core dependencies...")
    path_registry = PathRegistry()
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)
    print("Dependencies initialized.")

    # 2. Load the Static Recommender Model
    print("\n[PHASE 2] Loading the book recommender model...")
    model = persister.load(version="1.0")
    if not model:
        print("FATAL: Book model could not be loaded. Please run the model build script first.")
        return
    print("Book model loaded successfully.")

    # 3. Initialize All System Components
    print("\n[PHASE 3] Initializing all recommendation components...")
    # Repositories
    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)

    # FAISS Index for User Profiles
    index_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        print(f"FATAL: Could not resolve path for '{config.MODEL_ARTIFACTS_DIR_KEY}'. Aborting.")
        return
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)

    # Re-ranking strategies
    rerankers = [GenrePreferenceReRanker(), PageCountReRanker()]

    # Recommendation Engines
    content_recommender = ContentBasedRecommender(model, rerankers=rerankers)
    collaborative_recommender = CollaborativeFilteringRecommender(model, user_profile_index, rerankers=rerankers)

    # Taste Vector Calculator
    taste_vector_calculator = TasteVectorCalculator(model)
    print("All components initialized.")

    # 4. Instantiate and Prepare the Main Facade
    print("\n[PHASE 4] Instantiating and preparing the UserRecommenderFacade...")
    recommender_facade = UserRecommenderFacade(
        content_recommender=content_recommender,
        collaborative_recommender=collaborative_recommender,
        interaction_repo=interaction_repo,
        user_profile_repo=user_profile_repo,
        taste_vector_calculator=taste_vector_calculator,
        user_profile_index=user_profile_index
    )
    # This crucial step loads the FAISS index into memory
    recommender_facade.load_indices()
    print("Facade ready and user profile index loaded.")

    # 5. Generate and Display Recommendations
    print(f"\n--- Generating recommendations for user: {user_id} ---")

    # A) Content-Based Recommendations
    print("\n[STRATEGY 1] Running Content-Based Recommender...")
    content_based_recs = recommender_facade.recommend_with_content_based(user_id, top_n=5)
    if content_based_recs:
        print("Top 5 Content-Based Recommendations:")
        for i, title in enumerate(content_based_recs, 1):
            print(f"  {i}. {title}")
    else:
        print("Could not generate content-based recommendations.")

    # B) Collaborative Filtering Recommendations
    print("\n[STRATEGY 2] Running Scalable Collaborative Filtering Recommender...")
    collaborative_recs = recommender_facade.recommend_with_collaborative_filtering(user_id, top_n=5)
    if collaborative_recs:
        print("Top 5 Collaborative Filtering Recommendations:")
        for i, title in enumerate(collaborative_recs, 1):
            print(f"  {i}. {title}")
    else:
        print("Could not generate collaborative filtering recommendations.")
    
    print("\n--- Showcase Complete ---")


if __name__ == "__main__":
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    PathRegistry().set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    
    TEST_USER_ID = 'cristian'
    
    run_full_recommendation_showcase(TEST_USER_ID)