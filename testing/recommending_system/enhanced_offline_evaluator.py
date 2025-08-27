# testing/recommending_system/enhanced_offline_evaluator.py
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Set, Dict, Any, Optional, cast
from datetime import datetime
import warnings

# Import the base evaluator and all project components
from .offline_evaluator import OfflineEvaluator
from .genre_alignment_analyzer import GenreAlignmentAnalyzer
from recommender.repository import UserInteractionRepository, BookRepository
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


class EnhancedOfflineEvaluator(OfflineEvaluator):
    """
    Enhanced offline evaluation of the recommender system with genre alignment
    analysis and temporal vs random splitting comparison.
    """
    
    def __init__(
        self,
        facade: UserRecommenderFacade,
        interaction_repo: UserInteractionRepository,
        book_repo: BookRepository,
        split_method: str = 'random',
        k: int = 10,
        min_reviews: int = 10,
        holdout_size: int = 2,
        min_high_ratings_for_split: int = 4
    ):
        """
        Initialize the enhanced evaluator.
        
        Args:
            facade: The main recommender facade
            interaction_repo: Repository to fetch user data
            book_repo: Repository to fetch book details (NEW)
            split_method: 'random' or 'temporal' (NEW)
            k: Number of recommendations to generate
            min_reviews: Minimum reviews a user must have
            holdout_size: Number of highly-rated items to hide
            min_high_ratings_for_split: Minimum high ratings required for split
        """
        super().__init__(
            facade=facade,
            interaction_repo=interaction_repo,
            k=k,
            min_reviews=min_reviews,
            holdout_size=holdout_size,
            min_high_ratings_for_split=min_high_ratings_for_split
        )
        
        self.book_repo = book_repo
        self.split_method = split_method
        self.genre_analyzer = GenreAlignmentAnalyzer(book_repo, facade.model)
        self.logger = LoggerManager().get_logger()
        
        # Check if temporal data is available
        self.temporal_data_available = self._check_temporal_data_availability()
        
        if split_method == 'temporal' and not self.temporal_data_available:
            self.logger.warning("Temporal split requested but no temporal data available. Falling back to random split.")
            self.split_method = 'random'
    
    def _check_temporal_data_availability(self) -> bool:
        """
        Check if temporal data (review dates) is available in the database.
        
        Returns:
            True if temporal data is available, False otherwise
        """
        try:
            # Check a sample review for date fields
            sample = self.interaction_repo.db[self.interaction_repo.collection_name].find_one()
            if sample:
                return any(field in sample for field in ['date_added', 'date_updated', 'review_date'])
            return False
        except Exception as e:
            self.logger.error(f"Error checking temporal data availability: {e}")
            return False
    
    def _split_history_enhanced(self, history_df: pd.DataFrame, user_id: Any) -> Tuple[pd.DataFrame, Set[str]]:
        """
        Enhanced history splitting that supports both random and temporal methods.
        
        Args:
            history_df: User's complete interaction history
            user_id: User identifier for logging
            
        Returns:
            Tuple of (training_df, holdout_set)
        """
        if self.split_method == 'temporal' and self.temporal_data_available:
            return self._split_history_temporal(history_df, user_id)
        else:
            return self._split_history_random(history_df, user_id)
    
    def _split_history_temporal(self, history_df: pd.DataFrame, user_id: Any) -> Tuple[pd.DataFrame, Set[str]]:
        """
        Split user history based on temporal order (chronological).
        
        Args:
            history_df: User's complete interaction history
            user_id: User identifier for logging
            
        Returns:
            Tuple of (training_df, holdout_set)
        """
        # Try different possible date field names
        date_field = None
        for field in ['date_added', 'date_updated', 'review_date']:
            if field in history_df.columns:
                date_field = field
                break
        
        if not date_field:
            self.logger.warning(f"No date field found for user {user_id}, falling back to random split")
            return self._split_history_random(history_df, user_id)
        
        try:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(history_df[date_field]):
                # Handle common date formats including Unix date format like "Fri Aug 25 13:55:02 -0700 2017"
                # First try with automatic format inference
                try:
                    history_df[date_field] = pd.to_datetime(
                        history_df[date_field],
                        errors='coerce',
                        utc=True  # Handle timezone info properly
                    )
                except:
                    # Fallback without UTC conversion if it fails
                    history_df[date_field] = pd.to_datetime(
                        history_df[date_field],
                        errors='coerce'
                    )
            
            # Remove rows with invalid dates
            history_df = history_df.dropna(subset=[date_field])
            
            # Log successful parsing
            self.logger.info(f"Successfully parsed {len(history_df)} dates for user {user_id}")
            
            if history_df.empty:
                self.logger.warning(f"No valid dates for user {user_id}, falling back to random split")
                return self._split_history_random(history_df, user_id)
            
            # Sort by date
            history_df = history_df.sort_values(date_field)
            
            # Use 80-20 temporal split
            split_idx = int(len(history_df) * 0.8)
            if split_idx < 1:
                split_idx = 1
                
            training_df = history_df.iloc[:split_idx].copy()
            test_df = history_df.iloc[split_idx:].copy()
            
            # Get highly rated books from test set for holdout
            high_rated_test = test_df[test_df['rating'] >= 4.0]
            
            if len(high_rated_test) < self.holdout_size:
                self.logger.warning(f"Not enough high-rated test books for user {user_id}, using all available")
                holdout_set = set(high_rated_test['book_title'])
            else:
                # Take the required number of books from the test set
                holdout_df = high_rated_test.head(self.holdout_size)
                holdout_set = set(holdout_df['book_title'])
            
            return training_df, holdout_set
            
        except Exception as e:
            self.logger.error(f"Error in temporal split for user {user_id}: {e}")
            return self._split_history_random(history_df, user_id)
    
    def _split_history_random(self, history_df: pd.DataFrame, user_id: Any) -> Tuple[pd.DataFrame, Set[str]]:
        """
        Random history splitting (original method from base class).
        
        Args:
            history_df: User's complete interaction history
            user_id: User identifier for logging
            
        Returns:
            Tuple of (training_df, holdout_set)
        """
        return super()._split_history(history_df)
    
    def run_enhanced_evaluation(self, sample_size: int = 1000) -> pd.DataFrame:
        """
        Run the enhanced evaluation with genre alignment analysis.
        
        Args:
            sample_size: Number of users to evaluate
            
        Returns:
            DataFrame with enhanced evaluation results
        """
        self.logger.info(f"Starting enhanced evaluation with {self.split_method} splitting")
        
        test_user_ids = self._select_test_users(sample_size)
        if not test_user_ids:
            self.logger.error("No users found to evaluate. Aborting.")
            return pd.DataFrame()
        
        results = []
        all_recommended_items = set()
        genre_analysis_results = []
        
        for i, user_id in enumerate(test_user_ids):
            self.logger.info(f"Processing user {i+1}/{len(test_user_ids)}: {user_id}")
            
            try:
                # Get user history
                full_history = self.interaction_repo.find_interactions_by_user(user_id)
                training_df, holdout_set = self._split_history_enhanced(full_history, user_id)
                
                if not holdout_set:
                    self.logger.warning(f"Skipping user {user_id}: not enough data for hold-out.")
                    continue
                
                # Evaluate Content-Based Strategy
                cb_recs = self.facade.recommend_with_content_based(
                    user_id, top_n=self.k, override_history_df=training_df
                )
                cb_metrics = self._calculate_metrics(cb_recs, holdout_set)
                all_recommended_items.update(cb_recs)
                
                # Evaluate Collaborative Filtering Strategy
                cf_recs = self.facade.recommend_with_collaborative_filtering(
                    user_id, top_n=self.k, override_history_df=training_df
                )
                cf_metrics = self._calculate_metrics(cf_recs, holdout_set)
                all_recommended_items.update(cf_recs)
                
                # Genre Alignment Analysis for Content-Based
                cb_genre_analysis = self.genre_analyzer.analyze_user_recommendations(
                    user_id, full_history, cb_recs
                )
                
                # Genre Alignment Analysis for Collaborative Filtering
                cf_genre_analysis = self.genre_analyzer.analyze_user_recommendations(
                    user_id, full_history, cf_recs
                )
                
                # Store genre analysis results
                genre_analysis_results.extend([cb_genre_analysis, cf_genre_analysis])
                
                # Compile results
                user_result = {
                    'user_id': user_id,
                    'cb_precision': cb_metrics['precision_at_k'],
                    'cb_recall': cb_metrics['recall_at_k'],
                    'cf_precision': cf_metrics['precision_at_k'],
                    'cf_recall': cf_metrics['recall_at_k'],
                    'split_method': self.split_method,
                    'temporal_data_available': self.temporal_data_available,
                    # Content-Based Genre Metrics
                    'cb_genre_overlap': cb_genre_analysis['genre_overlap'],
                    'cb_genre_diversity': cb_genre_analysis['genre_diversity'],
                    'cb_popularity_bias': cb_genre_analysis['popularity_bias'],
                    'cb_genre_match_score': cb_genre_analysis['genre_match_score'],
                    # Collaborative Filtering Genre Metrics  
                    'cf_genre_overlap': cf_genre_analysis['genre_overlap'],
                    'cf_genre_diversity': cf_genre_analysis['genre_diversity'],
                    'cf_popularity_bias': cf_genre_analysis['popularity_bias'],
                    'cf_genre_match_score': cf_genre_analysis['genre_match_score'],
                }
                
                results.append(user_result)
                
            except Exception as e:
                self.logger.error(f"Error processing user {user_id}: {e}")
                continue
        
        if not results:
            self.logger.error("Evaluation finished but no results were collected.")
            return pd.DataFrame()
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Generate comprehensive report
        self._generate_enhanced_report(results_df, all_recommended_items, genre_analysis_results)
        
        return results_df
    
    def compare_splitting_methods(self, sample_size: int = 500) -> Dict[str, pd.DataFrame]:
        """
        Compare random vs temporal splitting methods.
        
        Args:
            sample_size: Number of users to evaluate for each method
            
        Returns:
            Dictionary containing results for both methods
        """
        if not self.temporal_data_available:
            self.logger.warning("Temporal data not available, cannot perform comparison")
            return {'random': self.run_enhanced_evaluation(sample_size)}
        
        results = {}
        
        # Test random splitting
        self.logger.info("Evaluating with random splitting...")
        original_method = self.split_method
        self.split_method = 'random'
        results['random'] = self.run_enhanced_evaluation(sample_size)
        
        # Test temporal splitting
        self.logger.info("Evaluating with temporal splitting...")
        self.split_method = 'temporal'
        results['temporal'] = self.run_enhanced_evaluation(sample_size)
        
        # Restore original method
        self.split_method = original_method
        
        # Generate comparison report
        self._generate_comparison_report(results)
        
        return results
    
    def _generate_enhanced_report(
        self, 
        results_df: pd.DataFrame, 
        all_recommended_items: set, 
        genre_analysis_results: List[Dict[str, Any]]
    ):
        """
        Generate comprehensive evaluation report.
        
        Args:
            results_df: DataFrame with evaluation results
            all_recommended_items: Set of all recommended items
            genre_analysis_results: List of genre analysis results
        """
        total_books_in_catalog = len(self.facade.model.idx_to_title)
        coverage = len(all_recommended_items) / total_books_in_catalog if total_books_in_catalog > 0 else 0
        
        # Aggregate genre analysis
        aggregated_genres = self.genre_analyzer.aggregate_analysis_results(genre_analysis_results)
        
        print("\n" + "="*70)
        print("ENHANCED OFFLINE EVALUATION COMPLETE")
        print("="*70)
        print(f"Evaluated on {len(results_df)} users (k={self.k})")
        print(f"Split Method: {self.split_method.upper()}")
        print(f"Temporal Data Available: {self.temporal_data_available}")
        
        # Basic metrics
        print("\nBASIC METRICS:")
        print("-"*40)
        basic_summary = {
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
        basic_df = pd.DataFrame(basic_summary)
        print(basic_df.to_string(index=False, float_format='%.3f'))
        
        # Genre alignment metrics
        if aggregated_genres:
            print("\nGENRE ALIGNMENT ANALYSIS:")
            print("-"*40)
            cb_genre_metrics = {
                'Content-Based Metrics': [
                    f"Genre Overlap: {results_df['cb_genre_overlap'].mean():.3f}",
                    f"Genre Diversity: {results_df['cb_genre_diversity'].mean():.1f}",
                    f"Popularity Bias: {results_df['cb_popularity_bias'].mean():.3f}",
                    f"Genre Match Score: {results_df['cb_genre_match_score'].mean():.3f}"
                ]
            }
            
            cf_genre_metrics = {
                'Collaborative Filtering Metrics': [
                    f"Genre Overlap: {results_df['cf_genre_overlap'].mean():.3f}",
                    f"Genre Diversity: {results_df['cf_genre_diversity'].mean():.1f}",
                    f"Popularity Bias: {results_df['cf_popularity_bias'].mean():.3f}",
                    f"Genre Match Score: {results_df['cf_genre_match_score'].mean():.3f}"
                ]
            }
            
            for metrics in [cb_genre_metrics, cf_genre_metrics]:
                for category, values in metrics.items():
                    print(f"\n{category}:")
                    for value in values:
                        print(f"  {value}")
        
        # Coverage and other metrics
        print(f"\nCatalog Coverage: {coverage:.2%} ({len(all_recommended_items)} unique items)")
        print("-"*70)
    
    def _generate_comparison_report(self, results: Dict[str, pd.DataFrame]):
        """
        Generate report comparing random vs temporal splitting.
        
        Args:
            results: Dictionary with results from both splitting methods
        """
        print("\n" + "="*70)
        print("SPLITTING METHOD COMPARISON")
        print("="*70)
        
        if 'random' not in results or 'temporal' not in results:
            print("Incomplete comparison data available.")
            return
        
        random_df = results['random']
        temporal_df = results['temporal']
        
        comparison_data = {
            'Metric': [
                'Precision@k (Content-Based)',
                'Recall@k (Content-Based)', 
                'Precision@k (Collaborative)',
                'Recall@k (Collaborative)',
                'Genre Overlap (Content-Based)',
                'Genre Overlap (Collaborative)',
                'Users Evaluated'
            ],
            'Random Split': [
                random_df['cb_precision'].mean(),
                random_df['cb_recall'].mean(),
                random_df['cf_precision'].mean(),
                random_df['cf_recall'].mean(),
                random_df['cb_genre_overlap'].mean() if 'cb_genre_overlap' in random_df.columns else 0,
                random_df['cf_genre_overlap'].mean() if 'cf_genre_overlap' in random_df.columns else 0,
                len(random_df)
            ],
            'Temporal Split': [
                temporal_df['cb_precision'].mean(),
                temporal_df['cb_recall'].mean(),
                temporal_df['cf_precision'].mean(),
                temporal_df['cf_recall'].mean(),
                temporal_df['cb_genre_overlap'].mean() if 'cb_genre_overlap' in temporal_df.columns else 0,
                temporal_df['cf_genre_overlap'].mean() if 'cf_genre_overlap' in temporal_df.columns else 0,
                len(temporal_df)
            ]
        }
        
        comparison_df = pd.DataFrame(comparison_data)
        print(comparison_df.to_string(index=False, float_format='%.3f'))
        
        print("\nINSIGHTS:")
        print("-"*20)
        print("• Random Split: Tests ability to fill gaps in user's known library")
        print("• Temporal Split: Tests ability to predict future reading preferences")
        print("• Lower temporal scores are expected and more realistic")
        print("-"*70)


def main():
    """Main function to demonstrate enhanced evaluation."""
    print("--- Initializing Enhanced Offline Evaluation System ---")
    
    # Initialize core dependencies
    path_registry = PathRegistry()
    path_registry.set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    path_registry.set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)
    
    # Load the model
    model = persister.load(version="1.0")
    if not model:
        print("FATAL: Book model could not be loaded. Please run the model build script first.")
        return
    
    # Initialize system components
    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)
    book_repo = BookRepository(db_conn)
    
    index_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        print(f"FATAL: Could not resolve path for '{config.MODEL_ARTIFACTS_DIR_KEY}'. Aborting.")
        return
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)
    
    rerankers = cast(List[Any], [PageCountReRanker()])
    content_recommender = ContentBasedRecommender(model, rerankers=rerankers)
    collaborative_recommender = CollaborativeFilteringRecommender(model, user_profile_index, rerankers=rerankers)
    taste_vector_calculator = TasteVectorCalculator(model)
    
    # Create facade
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
    
    # Run enhanced evaluation
    evaluator = EnhancedOfflineEvaluator(
        facade=recommender_facade,
        interaction_repo=interaction_repo,
        book_repo=book_repo,
        split_method='temporal',  # or 'temporal' 'random'
        k=10,
        min_reviews=15,
        holdout_size=3,
        min_high_ratings_for_split=8
    )
    
    # Single evaluation
    results = evaluator.run_enhanced_evaluation(sample_size=200)
    
    # Optional: Compare splitting methods
    # comparison = evaluator.compare_splitting_methods(sample_size=100)


if __name__ == "__main__":
    main()