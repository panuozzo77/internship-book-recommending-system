# data_processing/aggregators.py

import logging
from typing import List, Optional, Tuple, Dict, Any

from .book_data_provider_interface import BookDataProvider, BookMetadata
from core.utils.LoggerManager import LoggerManager

class MetadataAggregator:
    """
    Orchestra più provider per recuperare e consolidare i metadati di un libro.
    La sua unica responsabilità è aggregare i dati.
    """
    def __init__(self, providers: List[BookDataProvider]):
        self.providers = providers
        self.logger: logging.Logger = LoggerManager().get_logger()

    def fetch_best_metadata(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Tuple[Optional[BookMetadata], Dict[str, str]]:
        """
        Interroga tutti i provider e unisce i risultati, tracciando la fonte di ogni dato.
        """
        consolidated_data: BookMetadata = existing_data.copy() if existing_data else {}
        field_sources: Dict[str, str] = {}
        self.logger.info(f"Avvio aggregazione metadati per '{title}'...")

        for provider in self.providers:
            if all(key in consolidated_data for key in ["description", "page_count", "genres", "popular_shelves"]):
                self.logger.info("Tutti i dati chiave sono stati raccolti. Interruzione dell'aggregazione.")
                break

            self.logger.debug(f"Interrogazione provider: {provider.get_name()}")
            try:
                provider_data = provider.fetch_data(title, authors, existing_data=consolidated_data)
                if provider_data:
                    provider_name = provider.get_name()
                    for key, value in provider_data.items():
                        if value and (key not in consolidated_data or not consolidated_data.get(key)):
                            consolidated_data[key] = value  # type: ignore
                            field_sources[key] = provider_name
                            self.logger.debug(f"Dato '{key}' aggiunto da {provider_name}.")
            except Exception as e:
                self.logger.error(f"Errore durante l'interrogazione del provider {provider.get_name()}: {e}", exc_info=True)

        if consolidated_data:
            self.logger.info(f"Aggregazione completata per '{title}'. Campi raccolti: {list(consolidated_data.keys())}")
            return consolidated_data, field_sources
        
        self.logger.warning(f"Nessun dato trovato da nessun provider per '{title}'.")
        return None, {}