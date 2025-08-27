# recommender_v2/predictor.py

import os
import pickle
import numpy as np
import tensorflow as tf
import faiss

from . import config
from .model import BookModel

class Predictor:
    """Handles loading the trained Two-Tower model and making predictions."""

    def __init__(self, artifacts_path: str = config.MODEL_ARTIFACTS_DIR):
        self.artifacts_path = artifacts_path
        self.user_model = None
        self.book_model = None
        self.user_id_map = None
        self.book_id_map = None
        self.index = None
        self.load_artifacts()

    def load_artifacts(self):
        """Loads all necessary artifacts for prediction."""
        print("--- Loading Model Artifacts for Prediction ---")
        
        # Load user model
        user_model_path = os.path.join(self.artifacts_path, "user_model")
        self.user_model = tf.saved_model.load(user_model_path)
        print("User model loaded.")

        # Load book model
        book_model_path = os.path.join(self.artifacts_path, "book_model")
        self.book_model = tf.saved_model.load(book_model_path)
        print("Book model loaded.")

        # Load user ID mapping
        with open(os.path.join(self.artifacts_path, "user_id_map.pkl"), "rb") as f:
            self.user_id_map = pickle.load(f)
        print("User ID mapping loaded.")

        # Load book ID mapping
        with open(os.path.join(self.artifacts_path, "book_id_map.pkl"), "rb") as f:
            self.book_id_map = pickle.load(f)
        print("Book ID mapping loaded.")

        # Load FAISS index
        index_path = os.path.join(self.artifacts_path, config.FAISS_INDEX_FILENAME)
        self.index = faiss.read_index(index_path)
        print(f"FAISS index loaded. Contains {self.index.ntotal} vectors.")
        print("---------------------------------------------")

    def recommend(self, user_id: str, top_n: int = 10) -> list:
        """
        Generates top_n recommendations for a given user_id.
        """
        if self.user_model is None or self.user_id_map is None or self.index is None or self.book_id_map is None:
            raise RuntimeError("Model artifacts are not fully loaded.")

        # Map user ID string to integer index
        try:
            user_idx = self.user_id_map[user_id]
        except KeyError:
            print(f"User ID '{user_id}' not found in training data. Cannot generate recommendations.")
            return []
        
        # Get user embedding by calling the model's signature
        user_input_tensor = tf.constant([user_idx], dtype=tf.int64)
        # When a Keras model is saved, the default signature can be used for inference.
        inference_fn = self.user_model.signatures["serving_default"]
        user_embedding = inference_fn(user_input_tensor)['output_0']

        # Search the FAISS index
        distances, indices = self.index.search(np.array(user_embedding, dtype=np.float32), top_n)
        
        # Map indices back to book IDs
        # The book_id_map from the data_loader maps string IDs to integer indices.
        # We need the reverse mapping for FAISS results.
        id_to_book_id = {i: book_id for book_id, i in self.book_id_map.items()}
        recommended_book_ids = [id_to_book_id[i] for i in indices[0]]
        
        return recommended_book_ids

if __name__ == '__main__':
    # This is a placeholder for a showcase script.
    # You would typically run this after the trainer has created the artifacts.
    
    if not os.path.exists(config.MODEL_ARTIFACTS_DIR):
        print("Model artifacts not found. Please run the trainer first.")
    else:
        predictor = Predictor()
        
        # Example: Recommend for a user (replace with a real user ID from your dataset)
        # Note: This will only work if you have a user_id that was part of the training data.
        # For a real application, you'd need a way to map new users or use a default strategy.
        sample_user_id = "cristian" 
        
        # Get the first user from the map as a sample
        if predictor.user_id_map:
            sample_user_id = list(predictor.user_id_map.keys())[0]
            try:
                recommendations = predictor.recommend(sample_user_id)
                print(f"\nRecommendations for user '{sample_user_id}':")
                for book_id in recommendations:
                    print(f"  - {book_id}")
            except Exception as e:
                print(f"\nCould not generate recommendations. This is expected if the artifacts are not fully trained/available.")
                print(f"Error: {e}")
        else:
            print(f"\nCould not generate recommendations. This is expected if the artifacts are not fully trained/available.")
            pass