#!/usr/bin/env python3
# testing/recommending_system/run_enhanced_evaluation.py
"""
Usage example script for the Enhanced Offline Evaluator.

This script demonstrates how to use the enhanced evaluation system
to analyze your recommender system's performance with genre alignment
analysis and temporal vs random data splitting comparison.
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from enhanced_offline_evaluator import EnhancedOfflineEvaluator
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
from typing import cast, List, Any


def setup_system():
    """Initialize all system components."""
    print("🚀 Initializing Enhanced Evaluation System...")
    
    # Set up paths
    path_registry = PathRegistry()
    path_registry.set_path('config_file', 
                          '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    path_registry.set_path('processed_datasets_dir', 
                          '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    
    # Database connection
    db_conn = MongoDBConnection()
    
    # Load the recommender model
    persister = ModelPersister(path_registry)
    model = persister.load(version="1.0")
    if not model:
        raise RuntimeError("❌ FATAL: Book model could not be loaded. Please run the model build script first.")
    
    print("✅ Model loaded successfully")
    
    # Initialize repositories
    interaction_repo = UserInteractionRepository(db_conn)
    user_profile_repo = UserProfileRepository(db_conn)
    book_repo = BookRepository(db_conn)
    
    # Initialize FAISS index
    index_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
    if not index_dir:
        raise RuntimeError(f"❌ FATAL: Could not resolve path for '{config.MODEL_ARTIFACTS_DIR_KEY}'.")
    
    user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
    user_profile_index = UserProfileIndex(vector_size=model.vector_size, index_path=user_index_path)
    
    # Initialize recommenders
    rerankers = cast(List[Any], [PageCountReRanker(), GenrePreferenceReRanker()])
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
    
    # Load indices
    recommender_facade.load_indices()
    print("✅ System initialized successfully")
    
    return recommender_facade, interaction_repo, book_repo


def example_1_basic_enhanced_evaluation(facade, interaction_repo, book_repo):
    """Example 1: Basic enhanced evaluation with genre analysis."""
    print("\n" + "="*60)
    print("📊 EXAMPLE 1: Basic Enhanced Evaluation")
    print("="*60)
    
    evaluator = EnhancedOfflineEvaluator(
        facade=facade,
        interaction_repo=interaction_repo,
        book_repo=book_repo,
        split_method='random',
        k=10,
        min_reviews=15,
        holdout_size=3,
        min_high_ratings_for_split=8
    )
    
    print("Running evaluation on 100 users...")
    results = evaluator.run_enhanced_evaluation(sample_size=100)
    
    if not results.empty:
        print(f"✅ Evaluation completed! Results for {len(results)} users saved.")
        print(f"📈 Average Content-Based Precision@10: {results['cb_precision'].mean():.3f}")
        print(f"📈 Average Genre Overlap Score: {results['cb_genre_overlap'].mean():.3f}")
    else:
        print("❌ No results generated. Check your data and configuration.")
    
    return results


def example_2_temporal_evaluation(facade, interaction_repo, book_repo):
    """Example 2: Temporal split evaluation."""
    print("\n" + "="*60)
    print("⏰ EXAMPLE 2: Temporal Split Evaluation")
    print("="*60)
    
    evaluator = EnhancedOfflineEvaluator(
        facade=facade,
        interaction_repo=interaction_repo,
        book_repo=book_repo,
        split_method='temporal',
        k=10,
        min_reviews=20,
        holdout_size=2,
        min_high_ratings_for_split=6
    )
    
    if evaluator.temporal_data_available:
        print("✅ Temporal data is available. Running temporal evaluation...")
        results = evaluator.run_enhanced_evaluation(sample_size=50)
        
        if not results.empty:
            print(f"✅ Temporal evaluation completed! Results for {len(results)} users.")
        else:
            print("❌ No temporal results generated.")
    else:
        print("⚠️ Temporal data not available in database. Falling back to random split.")
        results = evaluator.run_enhanced_evaluation(sample_size=50)
    
    return results


def example_3_comparison_evaluation(facade, interaction_repo, book_repo):
    """Example 3: Compare random vs temporal splitting."""
    print("\n" + "="*60)
    print("🔄 EXAMPLE 3: Splitting Method Comparison")
    print("="*60)
    
    evaluator = EnhancedOfflineEvaluator(
        facade=facade,
        interaction_repo=interaction_repo,
        book_repo=book_repo,
        k=10,
        min_reviews=20,
        holdout_size=2
    )
    
    print("Running comparison between random and temporal splits...")
    comparison_results = evaluator.compare_splitting_methods(sample_size=50)
    
    if comparison_results:
        print("✅ Comparison completed!")
        
        for method, results in comparison_results.items():
            if not results.empty:
                print(f"\n📋 {method.upper()} Split Results:")
                print(f"   Users evaluated: {len(results)}")
                print(f"   Avg CB Precision: {results['cb_precision'].mean():.3f}")
                print(f"   Avg CB Genre Overlap: {results['cb_genre_overlap'].mean():.3f}")
    else:
        print("❌ Comparison failed. Check your configuration.")
    
    return comparison_results


def example_4_custom_configuration(facade, interaction_repo, book_repo):
    """Example 4: Custom configuration for specific analysis."""
    print("\n" + "="*60)
    print("⚙️ EXAMPLE 4: Custom Configuration")
    print("="*60)
    
    # Custom evaluator for power users with many reviews
    evaluator = EnhancedOfflineEvaluator(
        facade=facade,
        interaction_repo=interaction_repo,
        book_repo=book_repo,
        split_method='random',
        k=20,                    # More recommendations
        min_reviews=50,          # Power users only
        holdout_size=5,          # Larger test set
        min_high_ratings_for_split=15  # Need many high ratings
    )
    
    print("Evaluating power users (50+ reviews) with custom configuration...")
    results = evaluator.run_enhanced_evaluation(sample_size=30)
    
    if not results.empty:
        print(f"✅ Power user evaluation completed! Results for {len(results)} users.")
        print(f"📊 These users have complex reading patterns:")
        print(f"   Avg Genre Diversity: {results['cb_genre_diversity'].mean():.1f} genres per user")
        print(f"   Avg Popularity Bias: {results['cb_popularity_bias'].mean():.3f}")
    else:
        print("❌ Not enough power users found. Try lowering min_reviews.")
    
    return results


def analyze_results(results, title="Results Analysis"):
    """Helper function to analyze and display results."""
    if results.empty:
        print(f"❌ No results to analyze for {title}")
        return
    
    print(f"\n📈 {title}")
    print("-" * 50)
    
    # Basic metrics
    print("📊 Performance Metrics:")
    print(f"   Content-Based Precision@k: {results['cb_precision'].mean():.3f} ± {results['cb_precision'].std():.3f}")
    print(f"   Collaborative Precision@k: {results['cf_precision'].mean():.3f} ± {results['cf_precision'].std():.3f}")
    print(f"   Content-Based Recall@k: {results['cb_recall'].mean():.3f} ± {results['cb_recall'].std():.3f}")
    
    # Genre alignment metrics
    print("\n🎭 Genre Alignment Metrics:")
    print(f"   Genre Overlap (CB): {results['cb_genre_overlap'].mean():.3f} ± {results['cb_genre_overlap'].std():.3f}")
    print(f"   Genre Diversity (CB): {results['cb_genre_diversity'].mean():.1f} ± {results['cb_genre_diversity'].std():.1f}")
    print(f"   Popularity Bias (CB): {results['cb_popularity_bias'].mean():.3f} ± {results['cb_popularity_bias'].std():.3f}")
    print(f"   Genre Match Score (CB): {results['cb_genre_match_score'].mean():.3f} ± {results['cb_genre_match_score'].std():.3f}")
    
    # System insights
    print(f"\n🔍 System Insights:")
    print(f"   Users successfully evaluated: {len(results)}")
    print(f"   Split method used: {results['split_method'].iloc[0]}")
    print(f"   Temporal data available: {results['temporal_data_available'].iloc[0]}")


def main():
    """Main execution function."""
    print("🎯 Enhanced Offline Evaluation - Usage Examples")
    print("=" * 60)
    
    try:
        # Initialize system
        facade, interaction_repo, book_repo = setup_system()
        
        # Run examples
        print("\n🚀 Running evaluation examples...")
        
        # Example 1: Basic evaluation
        basic_results = example_1_basic_enhanced_evaluation(facade, interaction_repo, book_repo)
        analyze_results(basic_results, "Basic Enhanced Evaluation Results")
        
        # Example 2: Temporal evaluation
        temporal_results = example_2_temporal_evaluation(facade, interaction_repo, book_repo)
        analyze_results(temporal_results, "Temporal Evaluation Results")
        
        # Example 3: Comparison
        comparison_results = example_3_comparison_evaluation(facade, interaction_repo, book_repo)
        
        # Example 4: Custom configuration
        custom_results = example_4_custom_configuration(facade, interaction_repo, book_repo)
        analyze_results(custom_results, "Power User Analysis Results")
        
        print("\n" + "="*60)
        print("✅ All examples completed successfully!")
        print("💡 Check the output above for detailed results and insights.")
        print("📝 Consider saving results to CSV for further analysis:")
        print("   results.to_csv('enhanced_evaluation_results.csv', index=False)")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        print("💡 Please check:")
        print("   - MongoDB is running and accessible")
        print("   - The recommender model is built and saved")
        print("   - FAISS index is available")
        print("   - Database contains sufficient user interaction data")


if __name__ == "__main__":
    main()