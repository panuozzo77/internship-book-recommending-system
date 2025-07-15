from abc import ABC, abstractmethod
from typing import List, Dict, Optional, TypedDict, Union, Any
import re

# Definisce la struttura dei dati di una serie
class BookSeriesMetadata(TypedDict, total=False):
    id: str  # ID della serie fornito dal provider (es. "GR_SER_40582")
    name: str

# Definisce la struttura completa dei metadati che ogni provider può restituire.
# Usando `total=False`, tutti i campi sono opzionali.
class BookMetadata(TypedDict, total=False):
    # --- ID forniti dal provider ---
    provider_specific_id: str          # Es. "GR_BOOK_18115", "OL_WORK_OL27448W"
    provider_specific_author_id: str   # Es. "GR_AUTH_1077326"
    
    # --- Dati principali del libro ---
    title: str
    description: str
    page_count: int
    publisher: str
    publication_year: int
    
    # --- Dati collegati ---
    authors: List[str]                 # Lista di nomi di autori
    genres: List[str]                  # Generi in formato libero (es. ["Science Fiction", "Dystopian"])
    popular_shelves: List[Dict[str, Any]] # Es. [{"count": "123", "name": "to-read"}]
    series: BookSeriesMetadata         # Informazioni sulla serie a cui appartiene il libro

class BookDataProvider(ABC):
    """
    Interfaccia astratta per i provider di dati sui libri.
    Definisce i metodi che ogni provider concreto deve implementare.
    """
    @abstractmethod
    def get_name(self) -> str:
        """Restituisce il nome leggibile del provider (es. "GoogleBooks", "OpenLibrary")."""
        pass

    @abstractmethod
    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        """
        Cerca i metadati di un libro.

        Args:
            title: Il titolo del libro da cercare.
            authors: Una lista di nomi di autori associati.
            existing_data: Metadati già raccolti da altri provider o dal DB.
                           Il provider dovrebbe evitare di cercare dati già presenti e validi.

        Returns:
            Un dizionario `BookMetadata` con i dati trovati, o `None` se non trova nulla di utile.
        """
        pass

    def _normalize_genres(self, raw_genres: Union[List[str], str, None]) -> List[str]:
        """
        Metodo helper (concreto) per pulire e standardizzare una lista di generi.
        I provider possono usarlo per garantire un output consistente.
        """
        if not raw_genres:
            return []
        
        processed_genres = set()
        
        # Gestisce sia stringhe separate da delimitatori sia liste di stringhe
        if isinstance(raw_genres, str):
            genres_list = re.split(r'[,;/]', raw_genres)
        elif isinstance(raw_genres, list):
            genres_list = raw_genres
        else:
            return []

        # Pulisce ogni genere
        for g in genres_list:
            if isinstance(g, str):
                # Rimuove spazi extra, converte in minuscolo e rimuove frasi comuni
                genre_cleaned = g.strip().lower()
                genre_cleaned = re.sub(r'\s*\([^)]*\)', '', genre_cleaned) # Rimuove testo in parentesi
                if genre_cleaned:
                    processed_genres.add(genre_cleaned)
                    
        return sorted(list(processed_genres))