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
        """
        Costruisce una stringa pesata di tag e generi per un singolo libro.
        I tag vengono normalizzati (lowercase, trattini sostituiti da spazi).
        """
        content_parts = []
        
        # 1. Processa popular_shelves (filtrati per i top N)
        shelves = book_data.get('popular_shelves', [])
        if shelves:
            for shelf in shelves:
                name = shelf.get('name')
                if name and name in top_shelves:
                    try:
                        count = int(shelf.get('count', 0))
                        # Normalizza il nome e lo ripete per ponderazione
                        normalized_name = name.replace('-', ' ')
                        content_parts.extend([normalized_name] * count)
                    except (ValueError, TypeError):
                        continue # Ignora se il conteggio non è un numero valido
        
        # 2. Processa i generi dal dataset originale
        genres = book_data.get('genres', {})
        if genres:
            for genre_group, count in genres.items():
                # Separa generi composti come "history, historical fiction, biography"
                individual_genres = [g.strip() for g in genre_group.split(',')]
                for genre in individual_genres:
                    normalized_genre = genre.replace('-', ' ')
                    content_parts.extend([normalized_genre] * int(count))
        
        # 3. Processa i generi dallo scraping
        scraped_genres = book_data.get('scraped_genres', {})
        if scraped_genres:
            for genre, count in scraped_genres.items():
                normalized_genre = genre.replace('-', ' ')
                # Qui il conteggio è sempre 1, quindi basta aggiungere il genere
                content_parts.extend([normalized_genre] * int(count))
                
        return " ".join(content_parts)

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
            # Combina descrizione e titolo
            description = book.get('description', '') or ''
            title = book.get('book_title', '') or ''
            
            # Crea la stringa di tag e generi pesati
            tags_string = self._process_tags(book, top_shelves)
            
            # Combina tutto in un'unica stringa di "contenuto"
            # Dare più peso al titolo ripetendolo
            content = (f"{title} " * 3) + f"{description} {tags_string}"
            
            processed_data.append({
                'book_id': book.get('book_id'),
                'book_title': title,
                'page_count': book.get('page_count'),
                'content': content
            })
            
        df = pd.DataFrame(processed_data)
        df.dropna(subset=['book_id', 'book_title'], inplace=True)
        self.logger.info("Feature aggregation complete.")
        return df