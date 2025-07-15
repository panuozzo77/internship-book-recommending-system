# recommender/aggregator.py
import pandas as pd
from typing import Set, List
from .repository import BookRepository
from core.utils.LoggerManager import LoggerManager

class FeatureAggregator:
    """
    Responsabile dell'aggregazione e della preparazione delle feature
    per il modello di raccomandazione.
    """
    def __init__(self, book_repo: BookRepository, top_shelves_limit: int = 1000):
        self.book_repo = book_repo
        self.top_shelves_limit = top_shelves_limit
        self.logger = LoggerManager().get_logger()
        self._top_shelves_cache: Set[str] = set()

    def _get_top_shelves(self) -> Set[str]:
        """Recupera e memorizza nella cache i tag più popolari."""
        if not self._top_shelves_cache:
            self._top_shelves_cache = self.book_repo.get_top_popular_shelves(self.top_shelves_limit)
        return self._top_shelves_cache

    def _process_tags(self, book_data: dict, top_shelves: Set[str]) -> str:
        # Questo metodo ora è privato e restituisce solo la stringa per il TF-IDF
        # La logica di estrazione dei generi è stata spostata in un nuovo metodo.
        _, weighted_string = self._extract_genres_and_create_weighted_string(book_data, top_shelves)
        return weighted_string

    # --- NUOVO METODO PER ESTRARRE I GENERI E LA STRINGA PESATA ---
    def _extract_genres_and_create_weighted_string(self, book_data: dict, top_shelves: Set[str]) -> (Set[str], str):
        """
        Estrae un set di generi unici e normalizzati per un libro
        e costruisce anche la stringa pesata per il TF-IDF.
        """
        content_parts = []
        key_genres = set()
        
        # 1. Processa popular_shelves (filtrati per i top N)
        shelves = book_data.get('popular_shelves', [])
        if shelves:
            for shelf in shelves:
                name = shelf.get('name')
                if name and name in top_shelves:
                    normalized_name = name.replace('-', ' ').lower()
                    key_genres.add(normalized_name)
                    try:
                        count = int(shelf.get('count', 0))
                        content_parts.extend([normalized_name] * count)
                    except (ValueError, TypeError):
                        continue
        
        # 2. Processa i generi dal dataset originale
        genres = book_data.get('genres', {})
        if genres:
            for genre_group, count in genres.items():
                individual_genres = [g.strip().replace('-', ' ').lower() for g in genre_group.split(',')]
                for genre in individual_genres:
                    key_genres.add(genre)
                    content_parts.extend([genre] * int(count))
        
        # 3. Processa i generi dallo scraping
        scraped_genres = book_data.get('scraped_genres', {})
        if scraped_genres:
            for genre, count in scraped_genres.items():
                normalized_genre = genre.replace('-', ' ').lower()
                key_genres.add(normalized_genre)
                content_parts.extend([normalized_genre] * int(count))
                
        return key_genres, " ".join(content_parts)


    def aggregate_features_for_model(self) -> pd.DataFrame:
        """
        Orchestra il processo completo: carica dati, li processa e
        restituisce un DataFrame pronto per il ModelBuilder.
        """
        self.logger.info("Starting feature aggregation for model building...")
        top_shelves = self._get_top_shelves()
        all_books_data = self.book_repo.get_all_books_with_related_data()
        
        processed_data = []
        for book in all_books_data:
            description = book.get('description', '') or ''
            title = book.get('book_title', '') or ''
            
            # --- MODIFICATO ---
            # Estrae sia i generi chiave sia la stringa di tag pesati
            key_genres, tags_string = self._extract_genres_and_create_weighted_string(book, top_shelves)
            
            # Combina tutto in un'unica stringa di "contenuto"
            content = (f"{title} " * 3) + f"{description} {tags_string}"
            
            processed_data.append({
                'book_id': book.get('book_id'),
                'book_title': title,
                'page_count': book.get('page_count'),
                'content': content,
                'key_genres': list(key_genres) # Salviamo come lista per compatibilità con DataFrame
            })
            
        df = pd.DataFrame(processed_data)
        df.dropna(subset=['book_id', 'book_title'], inplace=True)
        self.logger.info("Feature aggregation complete.")
        return df