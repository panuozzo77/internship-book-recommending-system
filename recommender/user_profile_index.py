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
        
        self.index: Optional[faiss.IndexIDMap2] = None
        # This dictionary will hold the mapping from FAISS's integer IDs back to your original string user_ids
        self.int_to_str_id_map: dict[int, str] = {}
        # This dictionary will hold the reverse mapping, which is needed for adding new users
        self.str_to_int_id_map: dict[str, int] = {}

    def build(self, user_profiles: List[dict]):
        """
        Builds a new FAISS index from a list of user profiles.

        Args:
            user_profiles: A list of dictionaries, where each dictionary must
                           contain 'user_id' (int) and 'taste_vector' (np.ndarray).
        """
        if not user_profiles:
            self.logger.warning("Cannot build index from an empty list of profiles.")
            return

        self.logger.info(f"Building new FAISS index with {len(user_profiles)} user profiles...")
        
        # The core index for our vectors. IndexFlatL2 performs an exact search.
        # For massive datasets, one might use a faster, approximate index like IndexIVFFlat.
        core_index = faiss.IndexFlatL2(self.vector_size)
        self.index = faiss.IndexIDMap2(core_index)
        
        # Extract vectors and IDs into separate NumPy arrays for batch processing
        vectors = np.array([p['taste_vector'] for p in user_profiles], dtype=np.float32)
        user_ids = np.array([p['user_id'] for p in user_profiles], dtype=np.int64)

        # FAISS requires normalized vectors for efficient L2 -> cosine similarity search
        faiss.normalize_L2(vectors)
        
        # Add the vectors and their corresponding user_ids to the index
        self.index.add_with_ids(vectors, user_ids)
        
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

    def search(self, vector: np.ndarray, k: int) -> Optional[List[Tuple[int, float]]]:
        """
        Searches the index for the k nearest neighbors to a given vector.

        Args:
            vector: The query vector (the user seeking recommendations).
            k: The number of neighbors to find.

        Returns:
            A list of (user_id, distance) tuples, or None if the search fails.
        """
        if self.index is None:
            self.logger.error("Cannot search an uninitialized index.")
            return None

        # Reshape and normalize the query vector
        vector = vector.astype(np.float32).reshape(1, -1)
        faiss.normalize_L2(vector)
        
        # The search returns distances and the corresponding integer IDs
        distances, int_ids = self.index.search(vector, k)
        
        # The results are in NumPy arrays, so we process them into a list of tuples
        neighbors = []
        for i, int_id in enumerate(int_ids[0]):
            # -1 indicates that no neighbor was found (if k > ntotal)
            if int_id != -1:
                # Convert the internal integer ID back to the original string user_id
                str_user_id = self.int_to_str_id_map.get(int_id)
                if str_user_id:
                    # The distance is L2, but on normalized vectors, it can be converted
                    # to cosine similarity: sim = 1 - (dist^2 / 2)
                    similarity = 1 - (distances[0][i]**2) / 2
                    neighbors.append((str_user_id, similarity))
                
        return neighbors

    def save(self):
        """Saves the current index to the specified file path."""
        if self.index is None:
            self.logger.error("Cannot save an uninitialized index.")
            return
            
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        faiss.write_index(self.index, self.index_path)
        self.logger.info(f"FAISS index saved successfully to {self.index_path}")

    def load(self):
        """Loads an index and its corresponding ID map from the specified file paths."""
        if not os.path.exists(self.index_path) or not os.path.exists(self.map_path):
            self.logger.warning(f"FAISS index or ID map not found. A new index must be built.")
            return

        import joblib
        self.index = faiss.read_index(self.index_path)
        self.int_to_str_id_map = joblib.load(self.map_path)
        self.str_to_int_id_map = {v: k for k, v in self.int_to_str_id_map.items()} # Create reverse map
        
        self.logger.info(f"FAISS index and ID map loaded successfully. Total vectors: {self.index.ntotal}")