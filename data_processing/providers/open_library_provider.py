import requests
import time
import logging
from typing import List, Optional, Dict, Any

# Assumendo struttura: project_root/data_processing/providers/
# e project_root/utils/
# e project_root/data_processing/book_data_provider_interface.py
try:
    # Se eseguito come parte di un pacchetto (es. con -m)
    from ..book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.logger import LoggerManager # Assumendo utils sia a livello di project
except ImportError:
    # Fallback per esecuzione diretta dello script o se la struttura è diversa
    # Potrebbe essere necessario aggiungere il percorso al sys.path se questo fallisce
    from book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.logger import LoggerManager


class OpenLibraryProvider(BookDataProvider):
    SEARCH_API_URL = "https://openlibrary.org/search.json"
    WORKS_API_URL_TEMPLATE = "https://openlibrary.org/works/{work_id}.json"
    # OpenLibrary è abbastanza generosa, ma un piccolo ritardo è buona norma
    REQUEST_DELAY_SECONDS = 0.5 # Secondi tra le richieste

    def __init__(self):
        self.logger: logging.Logger = LoggerManager().get_logger()
        self.logger.debug(f"{self.get_name()} provider initialized.")

    def get_name(self) -> str:
        return "OpenLibrary"

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Helper per effettuare richieste HTTP gestendo errori base e delay."""
        time.sleep(self.REQUEST_DELAY_SECONDS)
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()  # Solleva HTTPError per codici 4xx/5xx
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{self.get_name()}: Errore richiesta API a {url}: {e}")
            return None
        except ValueError as e: # Include JSONDecodeError
            self.logger.error(f"{self.get_name()}: Errore parsing JSON da {url}: {e}")
            return None


    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        self.logger.info(f"{self.get_name()}: Ricerca per titolo '{title}', autori: '{', '.join(authors)}'")

        query_params: Dict[str, Any] = {"title": title}
        if authors:
            # OpenLibrary sembra gestire meglio un singolo nome autore nella query
            query_params["author"] = authors[0] 
            # Potresti provare a unire gli autori se preferisci: query_params["author"] = ", ".join(authors)

        search_data = self._make_request(self.SEARCH_API_URL, params=query_params)

        if not search_data or not search_data.get("docs"):
            self.logger.info(f"{self.get_name()}: Nessun risultato dalla ricerca per '{title}'.")
            return None

        # Prendi il primo risultato rilevante (OpenLibrary ordina per rilevanza di default)
        # Filtriamo per assicurarci che ci sia un `seed` che contenga l'ID dell'opera.
        # OpenLibrary può restituire edizioni o opere. Vogliamo l'opera per la descrizione.
        first_book_result = None
        for doc in search_data["docs"]:
            # L'ID dell'opera è spesso in doc.get("seed") come lista, es. ["/works/OL12345W"]
            # o a volte in doc.get("work_key") o doc.get("key") che inizia con /works/
            seed_keys = doc.get("seed", [])
            work_id_from_seed = None
            for seed_item in seed_keys:
                if isinstance(seed_item, str) and seed_item.startswith("/works/"):
                    work_id_from_seed = seed_item.replace("/works/", "")
                    break
            
            if work_id_from_seed:
                first_book_result = doc
                first_book_result["resolved_work_id"] = work_id_from_seed # Aggiungiamo per comodità
                break
            elif doc.get("key", "").startswith("/works/"): # Fallback se "seed" non c'è
                first_book_result = doc
                first_book_result["resolved_work_id"] = doc.get("key").replace("/works/", "")
                break
        
        if not first_book_result or not first_book_result.get("resolved_work_id"):
            self.logger.info(f"{self.get_name()}: Nessun risultato di ricerca con ID opera valido per '{title}'.")
            return None

        work_id = first_book_result["resolved_work_id"]
        self.logger.debug(f"{self.get_name()}: Trovato work ID '{work_id}' per '{title}'. Recupero dettagli dell'opera...")
        
        work_details_url = self.WORKS_API_URL_TEMPLATE.format(work_id=work_id)
        work_data = self._make_request(work_details_url)

        if not work_data:
            self.logger.warning(f"{self.get_name()}: Impossibile recuperare dettagli per l'opera {work_id}.")
            # Potremmo comunque provare a estrarre qualcosa da `first_book_result` se `work_data` fallisce.
            # Per ora, restituiamo None se i dettagli dell'opera non sono disponibili.
            return None

        collected_metadata: BookMetadata = {}

        # 1. Descrizione
        if not existing_data or "description" not in existing_data:
            description_value = work_data.get("description")
            if isinstance(description_value, dict) and "value" in description_value:
                collected_metadata["description"] = description_value["value"].strip()
            elif isinstance(description_value, str):
                collected_metadata["description"] = description_value.strip()
            if "description" in collected_metadata:
                 self.logger.debug(f"{self.get_name()}: Descrizione trovata: {collected_metadata['description'][:50]}...")

        # 2. Generi (dai soggetti/subjects)
        if not existing_data or "genres" not in existing_data:
            subjects = work_data.get("subjects", [])
            if subjects and isinstance(subjects, list):
                # _normalize_genres si aspetta una lista di stringhe
                normalized_genres = self._normalize_genres(subjects)
                if normalized_genres:
                    collected_metadata["genres"] = normalized_genres
                    self.logger.debug(f"{self.get_name()}: Generi trovati: {collected_metadata['genres']}")

        # 3. Numero di Pagine
        # Il numero di pagine è più spesso associato a una specifica *edizione* piuttosto che a un'*opera*.
        # Potremmo provare a prenderlo da `first_book_result` (che potrebbe essere un'edizione)
        # o iterare sulle edizioni se `work_data` le elenca.
        # Per semplicità, proviamo da `first_book_result` se è un campo numerico.
        if not existing_data or "page_count" not in existing_data:
            page_count_candidate = first_book_result.get("number_of_pages_median") # A volte presente
            if not page_count_candidate:
                 # Alcune edizioni potrebbero avere 'publish_date', 'publisher', 'isbn_13', 'number_of_pages'
                 # Ma `search.json` non sempre restituisce questi dettagli per ogni doc.
                 # Esempio: doc.get("edition_key") può essere usato per un'altra chiamata API ai dettagli dell'edizione.
                 # Per ora, ci affidiamo a 'number_of_pages_median' se disponibile.
                 pass # Non abbiamo un modo semplice e diretto per le pagine dall'opera o dalla ricerca base

            if page_count_candidate and isinstance(page_count_candidate, (int, float)):
                try:
                    collected_metadata["page_count"] = int(page_count_candidate)
                    self.logger.debug(f"{self.get_name()}: Numero pagine trovato: {collected_metadata['page_count']}")
                except ValueError:
                    self.logger.warning(f"{self.get_name()}: Impossibile convertire page_count '{page_count_candidate}' in intero.")


        if collected_metadata:
            self.logger.info(f"{self.get_name()}: Dati raccolti per '{title}': {list(collected_metadata.keys())}")
            return collected_metadata
        else:
            self.logger.info(f"{self.get_name()}: Nessun nuovo dato utile trovato o estraibile per '{title}'.")
            return None