# testing/recommending_system/genre_alignment_analyzer.py
import pandas as pd
import numpy as np
from typing import Dict, List, Set, Any, Optional
from collections import Counter

from recommender.repository import BookRepository
from recommender.model import RecommenderModel
from core.utils.LoggerManager import LoggerManager


class GenreAlignmentAnalyzer:
    """
    Analyzes the alignment between user preferences and recommended books
    based on genre information.
    """
    
    def __init__(self, book_repo: BookRepository, model: RecommenderModel):
        """
        Initialize the genre alignment analyzer.
        
        Args:
            book_repo: Repository for accessing book details
            model: The recommender model containing book metadata
        """
        self.book_repo = book_repo
        self.model = model
        self.logger = LoggerManager().get_logger()
    
    def analyze_user_recommendations(
        self, 
        user_id: Any,
        user_history: pd.DataFrame, 
        recommendations: List[str]
    ) -> Dict[str, Any]:
        """
        Performs comprehensive genre alignment analysis for a user's recommendations.
        
        Args:
            user_id: The user's ID
            user_history: User's interaction history DataFrame
            recommendations: List of recommended book titles
            
        Returns:
            Dictionary containing various alignment metrics
        """
        try:
            # Extract user's preferred genres from history
            user_genres = self._extract_user_preferred_genres(user_history)
            
            if not user_genres:
                self.logger.warning(f"No preferred genres found for user {user_id}")
                return self._empty_analysis_result()
            
            # Get genres for recommended books
            rec_genre_data = self._get_recommendation_genres(recommendations)
            
            if not rec_genre_data['all_genres']:
                self.logger.warning(f"No genres found for recommendations for user {user_id}")
                return self._empty_analysis_result()
            
            # Calculate alignment metrics
            overlap_score = self._calculate_genre_overlap(user_genres, rec_genre_data['all_genres'])
            diversity_score = self._calculate_genre_diversity(rec_genre_data['all_genres'])
            bias_score = self._calculate_popularity_bias(rec_genre_data['genre_counts'])
            match_score = self._calculate_genre_match_score(user_genres, rec_genre_data['per_book'])
            
            return {
                'user_id': user_id,
                'genre_overlap': overlap_score,
                'genre_diversity': diversity_score,
                'popularity_bias': bias_score,
                'genre_match_score': match_score,
                'user_preferred_genres': list(user_genres),
                'recommended_genres': list(rec_genre_data['all_genres']),
                'recommendations_with_genres': rec_genre_data['per_book'],
                'total_recommendations': len(recommendations),
                'recommendations_with_genre_data': len([x for x in rec_genre_data['per_book'] if x['genres']])
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing genre alignment for user {user_id}: {e}")
            return self._empty_analysis_result()
    
    def _extract_user_preferred_genres(self, user_history: pd.DataFrame) -> Set[str]:
        """
        Extract genres from books the user rated highly (>= 4.0).
        
        Args:
            user_history: User's interaction history
            
        Returns:
            Set of preferred genres
        """
        preferred_genres = set()
        
        # Filter for highly-rated books
        high_rated = user_history[user_history['rating'] >= 4.0]
        
        for _, row in high_rated.iterrows():
            # Get genres from the history DataFrame (includes joined genre data)
            genres_from_row = self._extract_genres_from_row(row)
            preferred_genres.update(genres_from_row)
            
            # Also try to get genres from model metadata if book title is available
            book_title = getattr(row, 'book_title', None)
            if book_title and book_title in self.model.title_to_idx:
                book_idx = self.model.title_to_idx[book_title]
                model_genres = self.model.book_metadata.iloc[book_idx]['key_genres']
                if isinstance(model_genres, list):
                    preferred_genres.update(model_genres)
        
        return preferred_genres
    
    def _extract_genres_from_row(self, row: pd.Series) -> Set[str]:
        """
        Extract normalized genres from a DataFrame row.
        
        Args:
            row: A row from user history DataFrame
            
        Returns:
            Set of normalized genre strings
        """
        genres = set()
        
        # Process 'genres' field (from book_genres collection)
        if hasattr(row, 'genres') and row.genres:
            if isinstance(row.genres, dict):
                for genre_group in row.genres:
                    individual_genres = [g.strip().replace('-', ' ').lower() 
                                       for g in genre_group.split(',')]
                    genres.update(individual_genres)
        
        # Process 'scraped_genres' field (from book_genres_scraped collection)
        if hasattr(row, 'scraped_genres') and row.scraped_genres:
            if isinstance(row.scraped_genres, dict):
                for genre in row.scraped_genres:
                    normalized_genre = genre.replace('-', ' ').lower()
                    genres.add(normalized_genre)
        
        return genres
    
    def _get_recommendation_genres(self, recommendations: List[str]) -> Dict[str, Any]:
        """
        Get genre information for a list of recommended books.
        
        Args:
            recommendations: List of book titles
            
        Returns:
            Dictionary with genre information
        """
        all_genres = set()
        genre_counts = Counter()
        per_book_data = []
        
        for book_title in recommendations:
            book_genres = set()
            
            # Try to get book_id first
            book_id = self.book_repo.get_book_id_by_title(book_title)
            
            if book_id:
                # Get detailed book information including genres
                book_details = self.book_repo.get_book_details_by_id(book_id)
                if book_details and book_details.get('genres'):
                    book_genres.update([g.lower() for g in book_details['genres']])
            
            # Also try to get genres from model metadata
            if book_title in self.model.title_to_idx:
                book_idx = self.model.title_to_idx[book_title]
                model_genres = self.model.book_metadata.iloc[book_idx]['key_genres']
                if isinstance(model_genres, list):
                    book_genres.update([g.lower() for g in model_genres])
            
            # Update counters and collections
            all_genres.update(book_genres)
            for genre in book_genres:
                genre_counts[genre] += 1
            
            per_book_data.append({
                'title': book_title,
                'genres': list(book_genres)
            })
        
        return {
            'all_genres': all_genres,
            'genre_counts': genre_counts,
            'per_book': per_book_data
        }
    
    def _calculate_genre_overlap(self, user_genres: Set[str], rec_genres: Set[str]) -> float:
        """
        Calculate the overlap between user's preferred genres and recommended genres.
        
        Args:
            user_genres: Set of user's preferred genres
            rec_genres: Set of genres in recommendations
            
        Returns:
            Overlap score (0.0 to 1.0)
        """
        if not user_genres or not rec_genres:
            return 0.0
        
        intersection = user_genres.intersection(rec_genres)
        union = user_genres.union(rec_genres)
        
        return len(intersection) / len(union) if union else 0.0
    
    def _calculate_genre_diversity(self, rec_genres: Set[str]) -> float:
        """
        Calculate the genre diversity of recommendations.
        
        Args:
            rec_genres: Set of genres in recommendations
            
        Returns:
            Diversity score (number of unique genres)
        """
        return len(rec_genres)
    
    def _calculate_popularity_bias(self, genre_counts: Counter) -> float:
        """
        Calculate the popularity bias in genre recommendations.
        Higher scores indicate bias toward popular genres.
        
        Args:
            genre_counts: Counter of genres in recommendations
            
        Returns:
            Bias score (0.0 to 1.0)
        """
        if not genre_counts:
            return 0.0
        
        # Calculate entropy to measure distribution evenness
        total = sum(genre_counts.values())
        if total == 0:
            return 0.0
        
        entropy = 0
        for count in genre_counts.values():
            if count > 0:
                p = count / total
                entropy -= p * np.log2(p)
        
        # Normalize entropy (max entropy = log2(number of genres))
        max_entropy = np.log2(len(genre_counts)) if len(genre_counts) > 1 else 1
        
        # Convert to bias score (1 - normalized_entropy)
        # High entropy = low bias, Low entropy = high bias
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0
        bias_score = 1 - normalized_entropy
        
        return bias_score
    
    def _calculate_genre_match_score(
        self, 
        user_genres: Set[str], 
        per_book_data: List[Dict[str, Any]]
    ) -> float:
        """
        Calculate how many recommended books match user's preferred genres.
        
        Args:
            user_genres: Set of user's preferred genres
            per_book_data: List of books with their genre information
            
        Returns:
            Match score (0.0 to 1.0)
        """
        if not per_book_data or not user_genres:
            return 0.0
        
        matching_books = 0
        total_books = len(per_book_data)
        
        for book in per_book_data:
            book_genres = set(book['genres'])
            if book_genres.intersection(user_genres):
                matching_books += 1
        
        return matching_books / total_books if total_books > 0 else 0.0
    
    def _empty_analysis_result(self) -> Dict[str, Any]:
        """
        Return an empty analysis result structure.
        
        Returns:
            Dictionary with zero/empty values
        """
        return {
            'genre_overlap': 0.0,
            'genre_diversity': 0.0,
            'popularity_bias': 0.0,
            'genre_match_score': 0.0,
            'user_preferred_genres': [],
            'recommended_genres': [],
            'recommendations_with_genres': [],
            'total_recommendations': 0,
            'recommendations_with_genre_data': 0
        }
    
    def aggregate_analysis_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate genre analysis results across multiple users.
        
        Args:
            results: List of individual user analysis results
            
        Returns:
            Dictionary with aggregated metrics
        """
        if not results:
            return {}
        
        valid_results = [r for r in results if r['total_recommendations'] > 0]
        
        if not valid_results:
            return {}
        
        return {
            'avg_genre_overlap': float(np.mean([r['genre_overlap'] for r in valid_results])),
            'avg_genre_diversity': float(np.mean([r['genre_diversity'] for r in valid_results])),
            'avg_popularity_bias': float(np.mean([r['popularity_bias'] for r in valid_results])),
            'avg_genre_match_score': float(np.mean([r['genre_match_score'] for r in valid_results])),
            'total_users_analyzed': len(valid_results),
            'avg_recommendations_per_user': float(np.mean([r['total_recommendations'] for r in valid_results])),
            'avg_recommendations_with_genre_data': float(np.mean([r['recommendations_with_genre_data'] for r in valid_results]))
        }