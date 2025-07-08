# recommender/visualizer.py
from typing import Set
import io
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any

from recommender.facade import UserRecommenderFacade
from core.utils.LoggerManager import LoggerManager
from recommender.repository import UserInteractionRepository

class UserProfileVisualizer:
    """
    Generates visual representations of a user's taste profile,
    such as a genre preference radar chart.
    """
    def __init__(self, recommender_facade: UserRecommenderFacade):
        self.facade = recommender_facade
        self.interaction_repo = self.facade.interaction_repo
        self.logger = LoggerManager().get_logger()

    def _get_genres_from_row(self, row: pd.Series) -> Set[str]:
        pure_genres = set()
        
        # Process 'genres' field (dict)
        genres_data = row.genres
        if isinstance(genres_data, dict):
            for genre_group in genres_data:
                individual_genres = [g.strip().replace('-', ' ').lower() for g in genre_group.split(',')]
                pure_genres.update(individual_genres)

        # Process 'scraped_genres' field (dict)
        #scraped_genres_data = row.scraped_genres
        #if isinstance(scraped_genres_data, dict):
        #    for genre in scraped_genres_data:
        #        normalized_genre = genre.replace('-', ' ').lower()
        #        pure_genres.add(normalized_genre)
        
        return pure_genres

    def _aggregate_genre_preferences(self, user_id: Any) -> Dict[str, float]:
        """
        Aggregates user's genre preferences based on their ratings.
        Returns a dictionary of {genre: preference_score}.
        """
        user_history_df = self.interaction_repo.find_interactions_by_user(user_id)
        if user_history_df.empty:
            self.logger.warning(f"No interaction history found for user {user_id}. Cannot generate profile.")
            return {}

        genre_scores = {}
        genre_counts = {}

        for row in user_history_df.itertuples():
            # No need to look up in the model anymore!
            # We get the genres directly from the enriched row data.
            book_genres = self._get_genres_from_row(row)
            if not book_genres:
                continue

            weight = float(row.rating) - 3.0
            
            for genre in book_genres:
                display_genre = genre.title() # Clean up for display
                genre_scores[display_genre] = genre_scores.get(display_genre, 0.0) + weight
                genre_counts[display_genre] = genre_counts.get(display_genre, 0) + 1
        
        # The rest of the logic is unchanged.
        final_scores = {}
        for genre, total_score in genre_scores.items():
            count = genre_counts.get(genre, 1)
            avg_score = total_score / count
            normalized_score = (avg_score + 2) * 25 
            final_scores[genre] = normalized_score
            
        return final_scores


    def create_preference_radar_chart(self, user_id: Any, top_n_genres: int = 6) -> io.BytesIO:
        """
        Generates a radar chart of the user's top genre preferences.

        Returns:
            A BytesIO buffer containing the PNG image data, or None if failed.
        """
        genre_preferences = self._aggregate_genre_preferences(user_id)

        if not genre_preferences:
            return None
        
        # Sort genres by preference and pick the top N
        sorted_genres = sorted(genre_preferences.items(), key=lambda item: item[1], reverse=True)
        
        # We need at least 3 axes for a radar chart to make sense
        if len(sorted_genres) < 3:
            self.logger.warning(f"User {user_id} has rated books in fewer than 3 genres. Radar chart is not suitable.")
            return None

        top_genres_data = dict(sorted_genres[:top_n_genres])
        labels = list(top_genres_data.keys())
        values = list(top_genres_data.values())
        
        num_vars = len(labels)

        # Calculate angle for each axis
        angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
        values += values[:1] # Close the plot
        angles += angles[:1]

        # Plotting
        fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
        
        # Set background and style to match the example
        fig.patch.set_facecolor('#E0E0E0')
        ax.set_facecolor('#E0E0E0')
        
        # Draw the main plot area
        ax.plot(angles, values, color='#FF8C69', linewidth=1)
        ax.fill(angles, values, color='#FF8C69', alpha=0.6)

        # Set axis labels
        ax.set_thetagrids(np.degrees(angles[:-1]), labels, color='grey', fontsize=9)

        # Set y-axis (radial) ticks and labels
        ax.set_ylim(0, 100)
        ax.set_yticks([25, 50, 75, 100])
        ax.set_yticklabels(["Dislike", "Neutral", "Like", "Love"], color="grey", fontsize=8)
        ax.set_rlabel_position(30)
        
        # Clean up grid lines
        ax.grid(color='white', linestyle='--', linewidth=0.5)
        
        ax.spines['polar'].set_color('white')

        # Save to a buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.5)
        plt.close(fig)
        buf.seek(0)
        
        self.logger.info(f"Successfully generated genre preference chart for user {user_id}.")
        return buf
    
        