import requests
import time
import logging
from typing import List, Optional, Dict, Any

try:
    from ..book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.LoggerManager import LoggerManager
except ImportError:
    from book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.LoggerManager import LoggerManager

class OpenLibraryProvider(BookDataProvider):
    SEARCH_API_URL = "https://openlibrary.org/search.json"
    WORKS_API_URL_TEMPLATE = "https://openlibrary.org/works/{work_id}.json"
    REQUEST_DELAY_SECONDS = 0.5

    def __init__(self):
        self.logger: logging.Logger = LoggerManager().get_logger()
        self.logger.debug(f"{self.get_name()} provider initialized.")

    def get_name(self) -> str:
        return "OpenLibrary"

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        time.sleep(self.REQUEST_DELAY_SECONDS)
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{self.get_name()}: API request error to {url}: {e}")
            return None
        except ValueError as e:
            self.logger.error(f"{self.get_name()}: JSON parsing error from {url}: {e}")
            return None

    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        self.logger.info(f"{self.get_name()}: Searching for title '{title}', authors: '{', '.join(authors)}'")
        
        query_params: Dict[str, Any] = {"title": title}
        if authors:
            query_params["author"] = authors[0]

        search_data = self._make_request(self.SEARCH_API_URL, params=query_params)
        if not search_data or not search_data.get("docs"):
            self.logger.info(f"{self.get_name()}: No results from search for '{title}'.")
            return None

        # Trova il primo risultato rilevante con un ID di "opera" (/works/)
        first_book_result = None
        work_id = None
        for doc in search_data["docs"]:
            key = doc.get("key", "")
            if key.startswith("/works/"):
                work_id = key.replace("/works/", "")
                first_book_result = doc
                break

        if not first_book_result or not work_id:
            self.logger.info(f"{self.get_name()}: No search result with a valid work ID for '{title}'.")
            return None

        self.logger.debug(f"{self.get_name()}: Found work ID '{work_id}'. Fetching work details...")
        work_data = self._make_request(self.WORKS_API_URL_TEMPLATE.format(work_id=work_id))
        if not work_data:
            self.logger.warning(f"{self.get_name()}: Could not retrieve details for work {work_id}.")
            return None

        # Costruisce l'oggetto BookMetadata con i dati trovati
        collected_metadata: BookMetadata = {"provider_specific_id": f"OL_WORK_{work_id}"}
        
        # Titolo (dall'opera, è più canonico)
        if work_data.get("title"):
            collected_metadata["title"] = work_data["title"]
        else:
            collected_metadata["title"] = first_book_result.get("title")

        # Descrizione
        description_value = work_data.get("description")
        if isinstance(description_value, dict):
            collected_metadata["description"] = description_value.get("value", "").strip()
        elif isinstance(description_value, str):
            collected_metadata["description"] = description_value.strip()

        # Generi e popular_shelves (OpenLibrary usa "subjects" per entrambi)
        subjects = work_data.get("subjects", [])
        if subjects:
            normalized_genres = self._normalize_genres(subjects)
            if normalized_genres:
                collected_metadata["genres"] = normalized_genres
                # Usiamo i soggetti come una stima di "popular shelves"
                collected_metadata["popular_shelves"] = [{"name": s, "count": "0"} for s in normalized_genres]

        # Numero di pagine (spesso nell'edizione, non nell'opera)
        page_count = first_book_result.get("number_of_pages_median")
        if page_count and isinstance(page_count, (int, float)):
            collected_metadata["page_count"] = int(page_count)

        # Anno di pubblicazione
        pub_year = first_book_result.get("first_publish_year")
        if pub_year and isinstance(pub_year, int):
            collected_metadata["publication_year"] = pub_year

        # Editore
        publishers = first_book_result.get("publisher", [])
        if publishers and isinstance(publishers, list):
            collected_metadata["publisher"] = publishers[0]

        # Autori (per confermare il nome canonico)
        author_names = first_book_result.get("author_name", [])
        if author_names:
            collected_metadata["authors"] = author_names

        # OpenLibrary non gestisce bene le serie in modo strutturato. Lo omettiamo.
        
        if len(collected_metadata) > 1: # Se abbiamo raccolto più del solo ID
            self.logger.info(f"{self.get_name()}: Data collected for '{title}': {list(collected_metadata.keys())}")
            return collected_metadata
        
        return None