# offline_evaluator.py
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Set, Dict

# --- Import all your project's components ---
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
from core.utils.LoggerManager import LoggerManager

class OfflineEvaluator:
    """
    Performs a rigorous offline evaluation of the recommender system
    using a hold-out testing strategy.
    """
    def __init__(
        self,
        facade: UserRecommenderFacade,
        interaction_repo: UserInteractionRepository,
        k: int = 10,
        min_reviews: int = 10,
        holdout_size: int = 2,
        min_high_ratings_for_split: int = 4
    ):
        """
        Initializes the evaluator.
        Args:
            facade: The main recommender facade.
            interaction_repo: Repository to fetch user data.
            k: The number of recommendations to generate (for Precision@k/Recall@k).
            min_reviews: The minimum number of reviews a user must have to be included.
            holdout_size: The number of highly-rated items to hide for testing.
        """
        self.facade = facade
        self.interaction_repo = interaction_repo
        self.k = k
        self.min_reviews = min_reviews
        self.holdout_size = holdout_size
        if min_high_ratings_for_split <= holdout_size:
            raise ValueError("min_high_ratings_for_split must be greater than holdout_size")
        self.min_high_ratings_for_split = min_high_ratings_for_split
        self.logger = LoggerManager().get_logger()

    def _select_test_users(self, sample_size: int = 1000) -> List[str]:
        """Selects a random sample of users who meet the minimum review criteria."""
        self.logger.info(f"Selecting up to {sample_size} users with > {self.min_reviews} reviews...")
        pipeline = [
            {'$group': {'_id': '$user_id', 'review_count': {'$sum': 1}}},
            {'$match': {'review_count': {'$gt': self.min_reviews}}},
            {'$sample': {'size': sample_size}}
        ]
        cursor = self.interaction_repo.db[self.interaction_repo.collection_name].aggregate(pipeline)
        user_ids = [doc['_id'] for doc in cursor]
        self.logger.info(f"Selected {len(user_ids)} users for evaluation.")
        return user_ids

    def _split_history(self, history_df: pd.DataFrame) -> Tuple[pd.DataFrame, Set[str]]:
        """
        Splits a user's history into a training set and a hold-out set.
        The hold-out set contains book titles the user rated highly.
        """
        high_ratings_df = history_df[history_df['rating'] >= 4.0]

        if len(high_ratings_df) < self.min_high_ratings_for_split:
            return history_df, set()

        holdout_df = high_ratings_df.sample(n=self.holdout_size, random_state=42)
        training_df = history_df.drop(holdout_df.index)
        
        holdout_set = set(holdout_df['book_title'])
        return training_df, holdout_set

    def _calculate_metrics(self, recommendations: List[str], holdout_set: Set[str]) -> Dict[str, float]:
        """Calculates Precision@k and Recall@k."""
        if not holdout_set:
            return {'precision_at_k': 0.0, 'recall_at_k': 0.0}

        hits = [rec for rec in recommendations if rec in holdout_set]
        num_hits = len(hits)

        precision = num_hits / self.k if self.k > 0 else 0.0
        recall = num_hits / len(holdout_set) if len(holdout_set) > 0 else 0.0
        
        return {'precision_at_k': precision, 'recall_at_k': recall}

    def run_full_evaluation(self, sample_size: int = 1000) -> pd.DataFrame:
        """Orchestrates the entire evaluation process."""
        test_user_ids = self._select_test_users(sample_size)
        if not test_user_ids:
            self.logger.error("No users found to evaluate. Aborting.")
            return pd.DataFrame()

        results = []
        all_recommended_items = set()

        for i, user_id in enumerate(test_user_ids):
            self.logger.info(f"Processing user {i+1}/{len(test_user_ids)}: {user_id}")
            full_history = self.interaction_repo.find_interactions_by_user(user_id)
            training_df, holdout_set = self._split_history(full_history)

            if not holdout_set:
                self.logger.warning(f"Skipping user {user_id}: not enough data for hold-out.")
                continue

            # --- Evaluate Content-Based Strategy ---
            cb_recs = self.facade.recommend_with_content_based(
                user_id, top_n=self.k, override_history_df=training_df
            )
            cb_metrics = self._calculate_metrics(cb_recs, holdout_set)
            all_recommended_items.update(cb_recs)
            
            # --- Evaluate Collaborative Filtering Strategy ---
            cf_recs = self.facade.recommend_with_collaborative_filtering(
                user_id, top_n=self.k, override_history_df=training_df
            )
            cf_metrics = self._calculate_metrics(cf_recs, holdout_set)
            all_recommended_items.update(cf_recs)

            results.append({
                'user_id': user_id,
                'cb_precision': cb_metrics['precision_at_k'],
                'cb_recall': cb_metrics['recall_at_k'],
                'cf_precision': cf_metrics['precision_at_k'],
                'cf_recall': cf_metrics['recall_at_k'],
            })
        
        if not results:
            self.logger.error("Evaluation finished but no results were collected.")
            return pd.DataFrame()

        # --- Aggregate and Report ---
        results_df = pd.DataFrame(results)
        total_books_in_catalog = len(self.facade.model.idx_to_title)
        coverage = len(all_recommended_items) / total_books_in_catalog if total_books_in_catalog > 0 else 0

        summary = {
            'Metric': ['Avg Precision@k', 'Avg Recall@k'],
            'Content-Based': [
                results_df['cb_precision'].mean(),
                results_df['cb_recall'].mean()
            ],
            'Collaborative': [
                results_df['cf_precision'].mean(),
                results_df['cf_recall'].mean()
            ]
        }
        summary_df = pd.DataFrame(summary)

        print("\n\n--- OFFLINE EVALUATION COMPLETE ---")
        print(f"Evaluated on {len(results_df)} users (k={self.k})\n")
        print(summary_df.to_string(index=False))
        print(f"\nCatalog Coverage: {coverage:.2%} ({len(all_recommended_items)} unique items recommended out of {total_books_in_catalog})")
        print("-----------------------------------")
        
        return results_df


def main():
    """Main function to initialize and run the evaluator."""
    print("--- Initializing System for Offline Evaluation ---")

    # 1. Initialize Core Dependencies
    path_registry = PathRegistry()
    # IMPORTANT: Set your paths correctly here
    path_registry.set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)
    
    # 2. Load the Model
    model = persister.load(version="1.0")
    if not model:
        print("FATAL: Book model could not be loaded. Please run the model build script first.")
        return

    # 3. Initialize All System Components (same as the showcase script)
    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)
    
    index_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)

    rerankers = [PageCountReRanker()]
    #rerankers = None
    content_recommender = ContentBasedRecommender(model, rerankers=rerankers)
    collaborative_recommender = CollaborativeFilteringRecommender(model, user_profile_index, user_profile_repo, rerankers=rerankers)
    taste_vector_calculator = TasteVectorCalculator(model)

    # 4. Instantiate the Facade
    recommender_facade = UserRecommenderFacade(
        content_recommender=content_recommender,
        collaborative_recommender=collaborative_recommender,
        interaction_repo=interaction_repo,
        user_profile_repo=user_profile_repo,
        taste_vector_calculator=taste_vector_calculator,
        user_profile_index=user_profile_index
    )
    recommender_facade.load_indices()
    print("System is ready.\n")

    # 5. Run the Evaluation
    evaluator = OfflineEvaluator(
        facade=recommender_facade,
        interaction_repo=interaction_repo,
        k=20,            # Recommend 10 items
        min_reviews=20,  # User must have at least 20 reviews
        holdout_size=5,   # Hide 5 books for testing
        min_high_ratings_for_split=10
    )
    evaluator.run_full_evaluation(sample_size=1000) # Test on a sample of 1000 users


if __name__ == "__main__":
    main()