from abc import ABC, abstractmethod
from typing import List, Dict, Optional, TypedDict, Literal, Union
import re

# Definisce la struttura dei metadati che ogni provider dovrebbe restituire
class BookMetadata(TypedDict, total=False):
    description: str
    page_count: int
    genres: List[str] # Lista di stringhe di genere, es: ["Fiction", "Science Fiction"]
    # Altri campi potrebbero essere: publisher, publication_date, isbn, etc.

# Stato per il log di augmentation_log a livello di libro
AugmentationStatus = Literal[
    "SUCCESS_FULL",         # Tutti i campi richiesti trovati
    "SUCCESS_PARTIAL",      # Alcuni campi trovati
    "NOT_FOUND",            # Nessun dato utile trovato da nessun provider
    "ALREADY_PROCESSED",    # Già processato con successo in precedenza
    "ERROR_PROVIDER",       # Errore durante l'interazione con un provider
    "ERROR_INTERNAL"        # Errore nello script di augmentation
]

# Stato per ogni tentativo di provider nel log
ProviderAttemptStatus = Literal["SUCCESS", "NOT_FOUND", "ERROR"]


class BookDataProvider(ABC):
    """
    Interfaccia astratta per i provider di dati sui libri.
    """
    @abstractmethod
    def get_name(self) -> str:
        """Restituisce il nome del provider (es. "GoogleBooks", "CalibreCLI")."""
        pass

    @abstractmethod
    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        """
        Cerca i dati del libro.
        Args:
            title: Il titolo del libro.
            authors: Una lista di nomi di autori.
            existing_data: Dati già raccolti da provider precedenti, per evitare di cercare info già presenti.
        Returns:
            Un dizionario BookMetadata con i dati trovati, o None se non trova nulla di utile.
        """
        pass

    def _normalize_genres(self, raw_genres: Union[List[str], str, None]) -> List[str]:
        """
        Metodo helper per normalizzare i generi in una lista di stringhe pulite.
        Può essere sovrascritto o utilizzato dai provider concreti.
        """
        if not raw_genres:
            return []
        
        processed_genres = set()
        if isinstance(raw_genres, str):
            # Split per virgola o punto e virgola, trimma spazi, converte in lowercase
            # e rimuove duplicati e stringhe vuote
            genres_list = re.split(r'[,;]', raw_genres)
        elif isinstance(raw_genres, list):
            genres_list = raw_genres
        else:
            return []

        for g in genres_list:
            if isinstance(g, str):
                genre_cleaned = g.strip().lower()
                if genre_cleaned:
                    processed_genres.add(genre_cleaned)
        return sorted(list(processed_genres))