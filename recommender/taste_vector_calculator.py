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

        liked_book_vectors = []
        
        # Itera sulle righe della cronologia utente
        for row in user_history_df.itertuples():
            title = getattr(row, 'book_title', None)
            rating = getattr(row, 'rating', None)

            if not title or rating is None:
                continue

            # --- MODIFICA CHIAVE: Considera solo i libri che sono piaciuti molto ---
            if float(rating) < 4.0:
                continue
            # --- FINE MODIFICA ---

            book_idx = self.model.title_to_idx.get(title)
            if book_idx is not None:
                # Aggiungi il vettore del libro piaciuto alla lista
                liked_book_vectors.append(self.model.index.get_item_vector(book_idx))
        
        if not liked_book_vectors:
            self.logger.warning(f"User {getattr(user_history_df, 'user_id', ['N/A'])[0]} has no highly-rated books. Cannot create profile.")
            return None

        # Il profilo finale è la semplice media dei vettori dei soli libri piaciuti.
        final_profile = np.mean(liked_book_vectors, axis=0, dtype=np.float32)
        
        self.logger.info(f"Taste vector calculated based on {len(liked_book_vectors)} liked books.")
        return final_profile