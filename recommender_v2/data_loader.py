# recommender_v2/data_loader.py

import pandas as pd
import tensorflow as tf
from typing import Tuple, Dict, List, Any

from recommender.repository import UserInteractionRepository
from etl.MongoDBConnection import MongoDBConnection

class DataLoader:
    """
    Handles loading and preprocessing of user-book interaction data
    to create TensorFlow Datasets for the Two-Tower model.
    """
    def __init__(self, interaction_repo: UserInteractionRepository):
        self.interaction_repo = interaction_repo
        self.user_id_map: Dict[str, int] = {}
        self.book_id_map: Dict[str, int] = {}
        self.id_user_map: Dict[int, str] = {}
        self.id_book_map: Dict[int, str] = {}

    def get_all_interactions(self) -> pd.DataFrame:
        """Fetches all user interactions from the repository."""
        return self.interaction_repo.find_all_interactions()

    def prepare_dataset(self, interactions_df: pd.DataFrame) -> Tuple[tf.data.Dataset, Dict[str, Any]]:
        """
        Prepares the TensorFlow dataset from the interactions DataFrame.
        It creates mappings for user and book IDs and converts the DataFrame
        into a dataset of features.
        """
        # Create mappings from string IDs to integer indices
        unique_user_ids = interactions_df['user_id'].unique()
        unique_book_ids = interactions_df['book_id'].unique()

        self.user_id_map = {id: i for i, id in enumerate(unique_user_ids)}
        self.book_id_map = {id: i for i, id in enumerate(unique_book_ids)}
        self.id_user_map = {i: id for id, i in self.user_id_map.items()}
        self.id_book_map = {i: id for id, i in self.book_id_map.items()}

        # Map string IDs to integer indices in the DataFrame
        interactions_df['user_idx'] = interactions_df['user_id'].map(self.user_id_map)
        interactions_df['book_idx'] = interactions_df['book_id'].map(self.book_id_map)

        # Create a dictionary of features
        features = {
            "user_idx": tf.constant(interactions_df['user_idx'].values, dtype=tf.int64),
            "book_idx": tf.constant(interactions_df['book_idx'].values, dtype=tf.int64),
        }

        # Create the TensorFlow dataset
        dataset = tf.data.Dataset.from_tensor_slices(features)
        return dataset, features

    def get_candidate_books_dataset(self) -> tf.data.Dataset:
        """
        Creates a TensorFlow dataset containing all unique book indices,
        to be used for candidate generation during prediction.
        """
        all_book_indices = list(self.book_id_map.values())
        candidate_dataset = tf.data.Dataset.from_tensor_slices(
            {"book_idx": tf.constant(all_book_indices, dtype=tf.int64)}
        )
        return candidate_dataset

if __name__ == '__main__':
    # Example usage
    print("--- Initializing DataLoader ---")
    db_conn = MongoDBConnection()
    interaction_repo = UserInteractionRepository(db_conn)
    
    data_loader = DataLoader(interaction_repo)
    
    print("Fetching interactions...")
    all_interactions = data_loader.get_all_interactions()
    print(f"Found {len(all_interactions)} total interactions.")
    
    if not all_interactions.empty:
        print("Preparing TensorFlow dataset...")
        tf_dataset, _ = data_loader.prepare_dataset(all_interactions)
        
        print("\n--- DataLoader Summary ---")
        print(f"Unique users: {len(data_loader.user_id_map)}")
        print(f"Unique books: {len(data_loader.book_id_map)}")
        
        print("\nExample from TensorFlow dataset:")
        for example in tf_dataset.take(5):
            print(f"  User Index: {example['user_idx'].numpy()}, Book Index: {example['book_idx'].numpy()}")
            
        print("\nCreating candidate books dataset...")
        candidates = data_loader.get_candidate_books_dataset()
        print(f"Candidate dataset size: {candidates.cardinality().numpy()}")
        print("--------------------------")