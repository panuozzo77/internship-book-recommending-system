# recommender/user_profile_index.py
import os
import faiss
import numpy as np
from typing import List, Tuple, Optional

from core.utils.LoggerManager import LoggerManager

class UserProfileIndex:
    """
    Manages a FAISS index for efficient similarity search of user profiles.
    This class handles building, loading, saving, and searching the index.
    """
    def __init__(self, vector_size: int, index_path: str):
        """
        Initializes the index manager.

        Args:
            vector_size: The dimensionality of the user profile vectors.
            index_path: The file path where the FAISS index is stored.
        """
        self.vector_size = vector_size
        self.index_path = index_path
        self.logger = LoggerManager().get_logger()
        self.map_path = self.index_path.replace('.faiss', '_id_map.joblib')
        
        self.index: Optional[faiss.IndexIDMap] = None
        # This dictionary will hold the mapping from FAISS's integer IDs back to your original string user_ids
        self.int_to_str_id_map: dict[int, str] = {}
        # This dictionary will hold the reverse mapping, which is needed for adding new users
        self.str_to_int_id_map: dict[str, int] = {}

    def build(self, user_profiles: List[dict]):
        """
        Builds a new FAISS index from a list of user profiles. It creates a consistent
        integer ID mapping to handle various user ID formats (str, int).

        Args:
            user_profiles: A list of dictionaries, where each must contain
                           'user_id' and 'taste_vector'.
        """
        self.logger.info(f"Building new FAISS index with {len(user_profiles)} user profiles...")
        
        core_index = faiss.IndexFlatL2(self.vector_size)
        self.index = faiss.IndexIDMap(core_index)
        self.int_to_str_id_map.clear()
        self.str_to_int_id_map.clear()

        if not user_profiles:
            self.logger.warning("Cannot build index from an empty list of profiles.")
            return

        # Prepare data and create consistent integer IDs
        vectors = np.array([p['taste_vector'] for p in user_profiles], dtype=np.float32)
        str_user_ids = [str(p['user_id']) for p in user_profiles]
        
        int_ids = np.arange(len(user_profiles), dtype=np.int64)
        self.int_to_str_id_map = {int(k): v for k, v in zip(int_ids, str_user_ids)}
        self.str_to_int_id_map = {v: k for k, v in self.int_to_str_id_map.items()}

        # Normalize vectors for cosine similarity search
        faiss.normalize_L2(vectors)
        
        # Add vectors with their new, consistent integer IDs
        self.index.add_with_ids(vectors, int_ids)
        
        self.logger.info(f"FAISS index built successfully. Total vectors in index: {self.index.ntotal}")

    def add(self, user_id: int, vector: np.ndarray):
        """
        Adds a single new user vector to the existing index.
        This is the key method for dynamic updates.

        Args:
            user_id: The user's unique ID.
            vector: The user's taste vector.
        """
        if self.index is None:
            self.logger.error("Cannot add to an uninitialized index. Build or load an index first.")
            return

        # Reshape and normalize the vector for FAISS
        vector = vector.astype(np.float32).reshape(1, -1)
        faiss.normalize_L2(vector)
        
        user_id_np = np.array([user_id], dtype=np.int64)
        
        # Add the new vector and its ID
        self.index.add_with_ids(vector, user_id_np)
        self.logger.info(f"Successfully added user_id {user_id} to the live FAISS index.")

    def search(self, vector: np.ndarray, k: int, user_id_to_exclude: Optional[str] = None) -> List[Tuple[str, float]]:
        """
        Searches for the k nearest neighbors, optionally excluding the query user.

        Args:
            vector: The query vector.
            k: The number of neighbors to find.
            user_id_to_exclude: The string ID of the user to exclude from results.

        Returns:
            A list of (user_id, similarity) tuples.
        """
        if self.index is None or self.index.ntotal == 0:
            self.logger.error("Cannot search an uninitialized or empty index.")
            return []

        # To exclude the user, we search for k+10 neighbors and filter.
        # This provides a larger buffer to ensure we get k results after exclusion.
        num_to_fetch = k + 10 if user_id_to_exclude is not None else k
        
        # Ensure k is not greater than the number of items in the index
        if num_to_fetch > self.index.ntotal:
            num_to_fetch = self.index.ntotal

        vector = vector.astype(np.float32).reshape(1, -1)
        faiss.normalize_L2(vector)
        
        distances, int_ids = self.index.search(vector, num_to_fetch)
        
        neighbors = []
        str_user_id_to_exclude = str(user_id_to_exclude) if user_id_to_exclude else None

        for i, int_id in enumerate(int_ids[0]):
            if int_id == -1:
                continue

            str_user_id = self.int_to_str_id_map.get(int_id)
            
            # Exclude the target user and ensure the ID mapping exists
            if str_user_id and str_user_id != str_user_id_to_exclude:
                similarity = 1 - (distances[0][i]**2) / 2
                neighbors.append((str_user_id, similarity))
                
        return neighbors[:k]

    def save(self):
        """Saves the current index and the ID map to their respective file paths."""
        if self.index is None:
            self.logger.error("Cannot save an uninitialized index.")
            return

        import joblib
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        
        # Save the FAISS index
        faiss.write_index(self.index, self.index_path)
        self.logger.info(f"FAISS index saved successfully to {self.index_path}")
        
        # Save the integer-to-string ID map
        joblib.dump(self.int_to_str_id_map, self.map_path)
        self.logger.info(f"ID map saved successfully to {self.map_path}")

    def load(self):
        """Loads an index and its corresponding ID map from the specified file paths."""
        if not os.path.exists(self.index_path) or not os.path.exists(self.map_path):
            self.logger.warning(f"FAISS index or ID map not found. A new index must be built.")
            return

        import joblib
        self.index = faiss.read_index(self.index_path)
        self.int_to_str_id_map = joblib.load(self.map_path)
        self.str_to_int_id_map = {v: k for k, v in self.int_to_str_id_map.items()} # Create reverse map
        
        if self.index:
            self.logger.info(f"FAISS index and ID map loaded successfully. Total vectors: {self.index.ntotal}")