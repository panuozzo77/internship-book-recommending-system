# recommender/facade.py
from typing import List, Any, Set, Tuple, Optional
import pandas as pd
import numpy as np

from recommender.engine import ContentBasedRecommender, CollaborativeFilteringRecommender
from recommender.repository import UserInteractionRepository
from recommender.user_profile_repository import UserProfileRepository
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.model import RecommenderModel
from recommender.user_profile_index import UserProfileIndex
from recommender import config

class UserRecommenderFacade:
    """
    Provides a unified interface for generating recommendations using different strategies.
    It orchestrates the interaction between data repositories, user profile management,
    and the underlying recommendation engines.
    """
    def __init__(
        self,
        content_recommender: ContentBasedRecommender,
        collaborative_recommender: CollaborativeFilteringRecommender,
        interaction_repo: UserInteractionRepository,
        user_profile_repo: UserProfileRepository,
        taste_vector_calculator: TasteVectorCalculator,
        user_profile_index: UserProfileIndex
    ):
        self.content_recommender = content_recommender
        self.collaborative_recommender = collaborative_recommender
        self.interaction_repo = interaction_repo
        self.user_profile_repo = user_profile_repo
        self.taste_vector_calculator = taste_vector_calculator
        self.user_profile_index = user_profile_index
        self.model: RecommenderModel = self.content_recommender.model

    def load_indices(self):
        """Loads the user profile FAISS index into memory."""
        self.user_profile_index.load()

    def recommend_with_content_based(self, user_id: Any, top_n: int = 10) -> List[str]:
        """
        Generates recommendations for a user based on their personal taste profile.
        """
        user_history_df = self.interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            return []

        profile_vector = self.taste_vector_calculator.calculate(user_history_df)
        if profile_vector is None:
            return []

        rerank_context, read_indices = self._prepare_rerank_context(user_history_df)
        
        return self.content_recommender.get_recommendations_by_profile(
            profile_vector=profile_vector,
            exclude_indices=read_indices,
            top_n=top_n,
            rerank_context=rerank_context
        )

    def recommend_with_collaborative_filtering(self, user_id: Any, top_n: int = 10) -> List[str]:
        """
        Generates recommendations by finding similar users via the FAISS index.
        """
        target_user_vector = self._get_or_create_user_profile(user_id)
        if target_user_vector is None:
            return []

        user_history_df = self.interaction_repo.find_interactions_by_user(user_id)
        rerank_context, read_indices = self._prepare_rerank_context(user_history_df)

        return self.collaborative_recommender.recommend(
            target_user_vector=target_user_vector,
            interaction_repo=self.interaction_repo,
            exclude_indices=read_indices,
            rerank_context=rerank_context,
            top_n=top_n,
            num_neighbors=config.COLLABORATIVE_N_NEIGHBORS
        )

    def _get_or_create_user_profile(self, user_id: Any) -> Optional[np.ndarray]:
        """
        Retrieves a user's taste vector. If the user is new, it calculates
        their profile, saves it to the database, and dynamically adds it to the
        live FAISS index.
        """
        # For existing users, their profile is already in the FAISS index.
        # We just need to fetch it from the DB for the query.
        profile_vector = self.user_profile_repo.find_by_user_id(user_id)
        if profile_vector is not None:
            return profile_vector
        
        # If not found, it's a new user. Calculate their profile.
        user_history_df = self.interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            return None
            
        new_profile_vector = self.taste_vector_calculator.calculate(user_history_df)
        
        if new_profile_vector is not None:
            # Save to DB for persistence
            self.user_profile_repo.save_or_update(user_id, new_profile_vector)
            
            # Dynamically add to the live FAISS index
            # This requires a mapping from the string user_id to a new integer ID
            str_id = str(user_id)
            if str_id not in self.user_profile_index.str_to_int_id_map:
                # Assign a new integer ID (the current size of the map)
                new_int_id = len(self.user_profile_index.str_to_int_id_map)
                self.user_profile_index.add(new_int_id, new_profile_vector)
                # Update the maps
                self.user_profile_index.int_to_str_id_map[new_int_id] = str_id
                self.user_profile_index.str_to_int_id_map[str_id] = new_int_id

        return new_profile_vector

    def _prepare_rerank_context(self, user_history_df: pd.DataFrame) -> Tuple[dict, Set[int]]:
        """
        Extracts information needed for re-ranking from a user's history.
        """
        if user_history_df.empty:
            return {}, set()

        liked_page_counts = []
        preferred_genres: Set[str] = set()
        disliked_genres: Set[str] = set()
        read_indices: Set[int] = set()

        for row in user_history_df.itertuples():
            title = getattr(row, 'book_title', None)
            rating = getattr(row, 'rating', 0.0)
            page_count = getattr(row, 'page_count', None)

            book_idx = self.model.title_to_idx.get(title)
            if book_idx is None:
                continue
            
            read_indices.add(book_idx)
            
            if rating >= 4 and pd.notna(page_count):
                liked_page_counts.append(float(page_count))

            book_genres = self.model.book_metadata.iloc[book_idx]['key_genres']
            if book_genres:
                if rating >= 4:
                    preferred_genres.update(book_genres)
                elif rating <= 2:
                    disliked_genres.update(book_genres)
        
        avg_page_count = np.mean(liked_page_counts) if liked_page_counts else 0.0
        
        rerank_context = {
            'avg_page_count': avg_page_count,
            'preferred_genres': preferred_genres,
            'disliked_genres': disliked_genres.difference(preferred_genres)
        }
        
        return rerank_context, read_indices