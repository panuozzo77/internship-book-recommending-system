import os
import requests
import time
from typing import List, Optional, Dict
from dotenv import load_dotenv
import logging # Import logging

# from data_processing.book_data_provider_interface import BookDataProvider, BookMetadata
from ..book_data_provider_interface import BookDataProvider, BookMetadata # Adattato per struttura project/data_processing/providers
from core.utils.LoggerManager import LoggerManager # Assumendo utils sia a livello di project

class GoogleBooksProvider(BookDataProvider):
    API_URL = "https://www.googleapis.com/books/v1/volumes"

    def __init__(self, api_key: Optional[str] = None):
        # Ottiene l'istanza del logger configurato
        self.logger: logging.Logger = LoggerManager().get_logger()
        if api_key:
            self.api_key = api_key
        else:
            load_dotenv()
            self.api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
        
        if not self.api_key:
            self.logger.error("GOOGLE_BOOKS_API_KEY non trovata. Il provider Google Books sarà disabilitato.")
        self.rate_limit_delay = 1

    def get_name(self) -> str:
        return "GoogleBooks"

    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        if not self.api_key:
            # self.logger.warning(f"Nessuna API key per {self.get_name()}, salto la ricerca per '{title}'.") # Già loggato in init
            return None

        query_parts = [f"intitle:{title}"]
        if authors:
            author_query_part = " ".join(authors)
            query_parts.append(f"inauthor:{author_query_part}")
        
        query = "+".join(query_parts)
        params = {'q': query, 'key': self.api_key, 'maxResults': 1, 'printType': 'books'}
        
        self.logger.debug(f"{self.get_name()}: Ricerca per '{title}' con autori '{', '.join(authors)}'. Query: {query}")

        try:
            response = requests.get(self.API_URL, params=params, timeout=10)
            time.sleep(self.rate_limit_delay)

            if response.status_code == 429:
                self.logger.error(f"{self.get_name()}: Errore 429 - Too Many Requests. Limite API raggiunto.")
                return None
            response.raise_for_status()

            data = response.json()
            if not data.get("items"):
                self.logger.info(f"{self.get_name()}: Nessun risultato per '{title}'.")
                return None

            volume_info = data["items"][0].get("volumeInfo", {})
            result: BookMetadata = {}

            if not existing_data or "description" not in existing_data:
                if volume_info.get("description"):
                    result["description"] = volume_info["description"]
            
            if not existing_data or "page_count" not in existing_data:
                if volume_info.get("pageCount"):
                    try:
                        result["page_count"] = int(volume_info["pageCount"])
                    except ValueError:
                        self.logger.warning(f"{self.get_name()}: pageCount non è un intero valido: {volume_info['pageCount']}")

            if not existing_data or "genres" not in existing_data:
                if volume_info.get("categories"):
                    result["genres"] = self._normalize_genres(volume_info["categories"])
            
            if result:
                self.logger.info(f"{self.get_name()}: Dati trovati per '{title}': {list(result.keys())}")
                return result
            else:
                self.logger.info(f"{self.get_name()}: Nessun nuovo dato trovato per '{title}' che non fosse già presente.")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"{self.get_name()}: Errore API durante la ricerca per '{title}': {e}")
            return None
        except Exception as e:
            self.logger.error(f"{self.get_name()}: Errore imprevisto durante la ricerca per '{title}': {e}", exc_info=True)
            return None