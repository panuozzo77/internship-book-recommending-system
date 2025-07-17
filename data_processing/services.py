# data_processing/services.py

import logging
from typing import Dict, Any, Optional, List
from .book_data_provider_interface import BookMetadata, BookSeriesMetadata

from .repositories import MongoBookRepository
from .aggregators import MetadataAggregator
from .genre_mapper import map_scraped_genres_to_predefined, LLMGenreMapper
from core.utils.LoggerManager import LoggerManager

class BookCreationService:
    """Contiene la logica di business per creare un nuovo libro."""
    def __init__(self, repository: MongoBookRepository, aggregator: MetadataAggregator, use_llm_mapper: bool = False, ollama_host: Optional[str] = None):
        self.repo = repository
        self.aggregator = aggregator
        self.logger: logging.Logger = LoggerManager().get_logger()
        self.use_llm_mapper = use_llm_mapper
        if self.use_llm_mapper and ollama_host:
            self.llm_mapper = LLMGenreMapper(ollama_host=ollama_host)
        else:
            self.llm_mapper = None

    def add_new_book(self, title: str, author_name: str) -> Dict[str, Any]:
        """Metodo pubblico per aggiungere un nuovo libro."""
        log_details = {"search_title": title, "search_author": author_name}
        self.logger.info(f"--- Richiesta di creazione per: '{title}' di '{author_name}' ---")

        if self.repo.find_book_by_title_author(title, author_name):
            self.logger.warning(f"Libro '{title}' di '{author_name}' già esistente. Creazione annullata.")
            self.repo.log_operation("addition", "DUPLICATE", log_details)
            return {"status": "DUPLICATE", "message": "Book with this title and author already exists."}

        metadata = self.aggregator.fetch_best_metadata(title, [author_name])
        if not metadata or not metadata.get("title"):
            self.logger.error(f"Dati insufficienti per '{title}'. Impossibile creare.")
            self.repo.log_operation("addition", "FAILED_NO_DATA", log_details)
            return {"status": "FAILED_NO_DATA", "message": "Could not retrieve metadata."}

        try:
            # Gestisce autori multipli
            author_names = metadata.get("authors", [author_name])
            author_ids = [self.repo.get_or_create_author(name) for name in author_names]
            # Filtra eventuali None se un autore non potesse essere creato
            author_ids = [aid for aid in author_ids if aid]

            series_data = metadata.get("series")
            series_id = self.repo.get_or_create_series(dict(series_data)) if series_data else None
            
            new_book_doc = self._build_book_document(metadata, author_ids, series_id)
            
            result = self.repo.add_book(new_book_doc)
            new_book_id = new_book_doc["book_id"] # L'ID è stato generato e aggiunto dal repository

            scraped_genres = metadata.get("genres", [])
            if scraped_genres:
                self.repo.add_scraped_genres(new_book_id, scraped_genres)
                if self.use_llm_mapper and self.llm_mapper:
                    mapped_genres = self.llm_mapper.map_genres(
                        title=metadata.get("title", ""),
                        authors=metadata.get("authors", []),
                        description=metadata.get("description", ""),
                        scraped_genres=scraped_genres
                    )
                else:
                    mapped_genres = map_scraped_genres_to_predefined(scraped_genres)
                
                if mapped_genres:
                    self.repo.add_genres(new_book_id, mapped_genres)

            self.repo.log_operation("addition", "SUCCESS", {"new_book_id": new_book_id, **log_details})
            return {"status": "SUCCESS", "book_id": new_book_id, "data": new_book_doc}
        except Exception as e:
            self.logger.critical(f"Errore durante la creazione del libro '{title}': {e}", exc_info=True)
            self.repo.log_operation("addition", "ERROR", {"reason": str(e), **log_details})
            return {"status": "ERROR", "message": str(e)}

    def _build_book_document(self, metadata: BookMetadata, author_ids: List[str], series_id: Optional[str]) -> Dict[str, Any]:
        """Helper per costruire il documento del libro da inserire."""
        authors_with_roles = [{"author_id": aid, "role": ""} for aid in author_ids]

        return {
            "author_id": authors_with_roles,
            "series": [series_id] if series_id else [],
            "book_title": metadata.get("title"),
            "description": metadata.get("description", ""),
            "page_count": metadata.get("page_count"),
            "publisher": metadata.get("publisher", ""),
            "year": metadata.get("publication_year"),
            "popular_shelves": metadata.get("popular_shelves", []),
            "work_id": "", "best_book_id": "", "day": "", "month": "",
            "average_rating": 0.0, "ratings_count": 0, "text_reviews_count": 0
        }

class BookUpdateService:
    """Contiene la logica di business per aggiornare un libro esistente."""
    def __init__(self, repository: MongoBookRepository, aggregator: MetadataAggregator, use_llm_mapper: bool = False, ollama_host: Optional[str] = None):
        self.repo = repository
        self.aggregator = aggregator
        self.logger: logging.Logger = LoggerManager().get_logger()
        self.use_llm_mapper = use_llm_mapper
        if self.use_llm_mapper and ollama_host:
            self.llm_mapper = LLMGenreMapper(ollama_host=ollama_host)
        else:
            self.llm_mapper = None
        
    def update_book(self, identifier: Dict[str, str]) -> Dict[str, Any]:
        """Metodo pubblico per aggiornare un libro, tramite ID o titolo/autore."""
        log_details = {"identifier": identifier}
        self.logger.info(f"--- Richiesta di aggiornamento per: {identifier} ---")

        book_doc = self._find_book(identifier)
        if not book_doc:
            self.logger.error(f"Libro non trovato per {identifier}. Aggiornamento annullato.")
            self.repo.log_operation("update", "NOT_FOUND", log_details)
            return {"status": "NOT_FOUND", "message": "Book not found."}

        book_id = book_doc["book_id"]
        author_doc = self.repo.authors_collection.find_one({"author_id": {"$in": book_doc["author_id"]}})
        author_name = author_doc["name"] if author_doc else ""

        # Crea una copia del documento del libro e rimuovi i generi esistenti
        # per forzare l'aggregatore a cercarli di nuovo.
        book_doc_for_aggregator = book_doc.copy()
        if "genres" in book_doc_for_aggregator:
            del book_doc_for_aggregator["genres"]

        metadata = self.aggregator.fetch_best_metadata(
            book_doc["book_title"],
            [author_name],
            existing_data=BookMetadata(**book_doc_for_aggregator)
        )
        if not metadata:
            self.logger.warning(f"Nessun nuovo dato trovato per aggiornare il libro ID {book_id}.")
            self.repo.log_operation("update", "NO_NEW_DATA", {"book_id": book_id, **log_details})
            return {"status": "NO_NEW_DATA", "message": "No new metadata found."}
        
        update_payload = self._create_update_payload(book_doc, metadata)
        
        # Gestione aggiornamento generi
        scraped_genres = metadata.get("genres")
        if scraped_genres:
            self.logger.info(f"Aggiornamento generi per il libro ID {book_id}...")
            self.repo.upsert_scraped_genres(book_id, scraped_genres)
            
            if self.use_llm_mapper and self.llm_mapper:
                mapped_genres = self.llm_mapper.map_genres(
                    title=metadata.get("title", book_doc.get("book_title", "")),
                    authors=metadata.get("authors", [author_name]),
                    description=metadata.get("description", book_doc.get("description", "")),
                    scraped_genres=scraped_genres
                )
            else:
                mapped_genres = map_scraped_genres_to_predefined(scraped_genres)
            
            if mapped_genres:
                self.repo.upsert_genres(book_id, mapped_genres)
                self.logger.info(f"Generi mappati per il libro ID {book_id} salvati.")

        if not update_payload:
            self.logger.info(f"Nessun campo da aggiornare per il libro ID {book_id}.")
            self.repo.log_operation("update", "NO_CHANGES_NEEDED", {"book_id": book_id, **log_details})
            return {"status": "NO_CHANGES_NEEDED", "message": "The book is already up-to-date."}
            
        try:
            result = self.repo.update_book(book_id, update_payload)
            if result.modified_count > 0:
                self.logger.info(f"Libro ID {book_id} aggiornato con successo. Campi: {list(update_payload.keys())}")
                self.repo.log_operation("update", "SUCCESS", {"book_id": book_id, "updated_fields": list(update_payload.keys()), **log_details})
                return {"status": "SUCCESS", "book_id": book_id, "updated_fields": list(update_payload.keys())}
            else:
                return {"status": "NO_CHANGES_NEEDED", "message": "The book is already up-to-date."}
        except Exception as e:
            self.logger.critical(f"Errore durante l'aggiornamento del libro ID {book_id}: {e}", exc_info=True)
            self.repo.log_operation("update", "ERROR", {"book_id": book_id, "reason": str(e), **log_details})
            return {"status": "ERROR", "message": str(e)}
            
    def _find_book(self, identifier: Dict[str, str]) -> Optional[Dict[str, Any]]:
        if "book_id" in identifier:
            return self.repo.find_book_by_id(identifier["book_id"])
        elif "title" in identifier and "author" in identifier:
            return self.repo.find_book_by_title_author(identifier["title"], identifier["author"])
        return None

    def _create_update_payload(self, old_doc: Dict[str, Any], new_metadata: BookMetadata) -> Dict[str, Any]:
        """Confronta i dati vecchi e nuovi per creare il payload di aggiornamento."""
        payload: Dict[str, Any] = {}
        # Itera solo su chiavi valide di BookMetadata per sicurezza
        for key in BookMetadata.__annotations__.keys():
            new_value = new_metadata.get(key)
            if new_value and new_value != old_doc.get(key):
                # Logica di aggiornamento selettiva
                if key in ["description", "page_count", "publisher", "publication_year", "popular_shelves", "genres", "series"]:
                    payload[key] = new_value
        
        # Rinomina publication_year in year se presente
        if "publication_year" in payload:
            payload["year"] = payload.pop("publication_year")
            
        return payload