# recommender/taste_vector_calculator.py
import numpy as np
import pandas as pd
from typing import Optional

from recommender.model import RecommenderModel
from core.utils.LoggerManager import LoggerManager

class TasteVectorCalculator:
    """
    This class is responsible for creating a user's taste vector based on their
    interaction history (e.g., book ratings).
    """
    def __init__(self, model: RecommenderModel):
        """
        Initializes the calculator.

        Args:
            model: A RecommenderModel instance to access book vectors and metadata.
        """
        self.model = model
        self.logger = LoggerManager().get_logger()
        self.logger.info("TasteVectorCalculator initialized.")

    def calculate(self, user_history_df: pd.DataFrame) -> Optional[np.ndarray]:
        """
        Calculates a single taste vector representing a user's preferences.

        The vector is a weighted average of the vectors of books the user has
        rated. The weight is determined by the rating:
        - Ratings > 3 contribute positively.
        - Ratings < 3 contribute negatively.
        - Ratings = 3 are neutral and have no weight.

        Args:
            user_history_df: A DataFrame containing the user's interaction
                             history. It must include 'book_title' and 'rating' columns.

        Returns:
            A NumPy array representing the user's taste vector, or None if a
            profile cannot be created.
        """
        if user_history_df.empty:
            self.logger.warning("Cannot calculate taste vector from empty history.")
            return None

        profile_accumulator = np.zeros(self.model.vector_size, dtype=np.float32)
        total_weight_magnitude = 0.0
        
        # Get a set of valid book indices from the user's history
        valid_book_indices = set()

        for row in user_history_df.itertuples():
            # Explicitly access attributes by name to help the type checker
            title = getattr(row, 'book_title', None)
            rating = getattr(row, 'rating', None)

            if not title or rating is None:
                continue

            book_idx = self.model.title_to_idx.get(title)
            if book_idx is None:
                continue
            
            valid_book_indices.add(book_idx)
            book_vector = self.model.index.get_item_vector(book_idx)
            
            # Calculate weight based on rating (from -1 to +1, with 3 as neutral)
            weight = (float(rating) - 3.0) / 2.0
            
            profile_accumulator += np.array(book_vector, dtype=np.float32) * weight
            total_weight_magnitude += abs(weight)

        if not valid_book_indices:
            self.logger.error("None of the books in the user's history were found in the model.")
            return None

        # If all ratings were neutral (3.0), the total weight is zero.
        # In this case, we fall back to a simple, unweighted average.
        if total_weight_magnitude == 0:
            self.logger.warning("User profile is neutral. Creating profile based on a simple average (fallback).")
            
            if not valid_book_indices: return None # Should not happen, but for safety

            read_vectors = [self.model.index.get_item_vector(i) for i in valid_book_indices]
            final_profile = np.mean(read_vectors, axis=0, dtype=np.float32)
        else:
            final_profile = profile_accumulator / total_weight_magnitude
        
        self.logger.info(f"Taste vector calculated successfully based on {len(valid_book_indices)} books.")
        return final_profile