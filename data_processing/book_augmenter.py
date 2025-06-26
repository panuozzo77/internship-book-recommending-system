import csv
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
import logging # Import logging

from pymongo.database import Database
from pymongo.errors import PyMongoError

# from data_processing.book_data_provider_interface import BookDataProvider, BookMetadata, AugmentationStatus, ProviderAttemptStatus
from .book_data_provider_interface import BookDataProvider, BookMetadata, AugmentationStatus, ProviderAttemptStatus # Adattato
from core.utils.LoggerManager import LoggerManager # Assumendo utils sia a livello di project


class BookAugmenter:
    def __init__(self, db: Database, providers: List[BookDataProvider]):
        self.db = db
        self.providers = providers
        self.logger: logging.Logger = LoggerManager().get_logger() # Ottiene il logger configurato

        self.books_collection = self.db.books
        self.augmentation_log_collection = self.db.augmentation_log
        self.book_genres_scraped_collection = self.db.book_genres_scraped

        try:
            self.augmentation_log_collection.create_index("book_id", unique=True)
            self.book_genres_scraped_collection.create_index("book_id", unique=True)
            self.logger.info("Indici per 'augmentation_log' e 'book_genres_scraped' assicurati.")
        except PyMongoError as e:
            self.logger.error(f"Errore durante la creazione degli indici: {e}")

    def _parse_authors_from_csv_string(self, authors_str: str) -> List[str]:
        if not authors_str or authors_str.lower() == "unknown author":
            return []
        return [author.strip() for author in authors_str.split(',') if author.strip()]

    def _update_database(self, book_id: str, collected_data: BookMetadata) -> Tuple[List[str], bool]:
        updated_book_fields = []
        genres_updated = False

        book_update_payload = {}
        if "description" in collected_data and collected_data["description"]:
            book_update_payload["description"] = collected_data["description"]
        if "page_count" in collected_data and collected_data["page_count"] is not None and collected_data["page_count"] > 0:
            book_update_payload["page_count"] = collected_data["page_count"]

        if book_update_payload:
            try:
                result = self.books_collection.update_one(
                    {"book_id": book_id}, {"$set": book_update_payload}
                )
                if result.modified_count > 0:
                    updated_book_fields = list(book_update_payload.keys())
                    self.logger.info(f"DB Update: Libro {book_id} aggiornato in 'books' con campi: {updated_book_fields}")
                elif result.matched_count > 0 and result.modified_count == 0 :
                     self.logger.info(f"DB Update: Libro {book_id} in 'books' già conteneva i dati aggiornati per {list(book_update_payload.keys())}.")
                elif result.matched_count == 0:
                     self.logger.warning(f"DB Update: Libro {book_id} non trovato in 'books' collection per aggiornamento.")
            except PyMongoError as e:
                self.logger.error(f"DB Update: Errore aggiornando libro {book_id} in 'books': {e}")
        
        if "genres" in collected_data and collected_data["genres"]:
            # La normalizzazione (lowercase, strip) è già fatta da _normalize_genres nel provider
            genres_dict_to_save = {genre: 1 for genre in collected_data["genres"] if genre} # Assicura che i generi siano validi
            if genres_dict_to_save:
                try:
                    update_result = self.book_genres_scraped_collection.update_one(
                        {"book_id": book_id},
                        {"$set": {"book_id": book_id, "genres": genres_dict_to_save, "last_updated": datetime.now(timezone.utc)}},
                        upsert=True
                    )
                    if update_result.upserted_id or update_result.modified_count > 0:
                        genres_updated = True
                        self.logger.info(f"DB Update: Generi per libro {book_id} salvati/aggiornati in 'book_genres_scraped'.")
                    elif update_result.matched_count > 0 and update_result.modified_count == 0:
                         self.logger.info(f"DB Update: Generi per libro {book_id} in 'book_genres_scraped' erano già aggiornati.")
                except PyMongoError as e:
                    self.logger.error(f"DB Update: Errore salvando generi per libro {book_id} in 'book_genres_scraped': {e}")
        
        return updated_book_fields, genres_updated

    def _log_augmentation_details(self, book_id: str, overall_status: AugmentationStatus,
                                 provider_attempts: List[Dict],
                                 fields_updated_in_books: List[str],
                                 genres_added_or_updated: bool):
        log_entry = {
            "book_id": book_id, "overall_status": overall_status,
            "last_processed_timestamp": datetime.now(timezone.utc),
            "provider_attempts": provider_attempts,
            "fields_updated_in_books_collection": fields_updated_in_books,
            "genres_added_to_scrape_collection": genres_added_or_updated,
        }
        try:
            self.augmentation_log_collection.update_one(
                {"book_id": book_id}, {"$set": log_entry}, upsert=True
            )
        except PyMongoError as e:
            self.logger.error(f"Log Error: Impossibile salvare log per {book_id}: {e}")

    def process_book_row(self, book_row: Dict):
        book_id = book_row.get("book_id")
        title = book_row.get("book_title")
        authors_csv_str = book_row.get("author_name", "")

        if not book_id or not title:
            self.logger.warning(f"Riga CSV saltata: book_id o book_title mancanti. Dati: {book_row}")
            return

        self.logger.info(f"--- Inizio processamento per Book ID: {book_id}, Titolo: '{title}' ---")
        
        try:
            existing_log = self.augmentation_log_collection.find_one({"book_id": book_id})
            if existing_log and existing_log.get("overall_status") == "SUCCESS_FULL":
                self.logger.info(f"Libro {book_id} già processato con successo (SUCCESS_FULL). Salto.")
                return
        except PyMongoError as e:
            self.logger.error(f"Errore nel controllo del log esistente per {book_id}: {e}")

        authors = self._parse_authors_from_csv_string(authors_csv_str)
        collected_data: BookMetadata = {}
        provider_attempts_log: List[Dict] = []
        
        # Carica dati esistenti dal DB per passarli a `existing_data` dei provider
        # Questo aiuta i provider a non cercare info già presenti nel DB principale
        try:
            current_book_db_doc = self.books_collection.find_one({"book_id": book_id}, {"description": 1, "page_count": 1})
            if current_book_db_doc:
                if current_book_db_doc.get("description"):
                    collected_data["description"] = current_book_db_doc["description"]
                if current_book_db_doc.get("page_count"):
                    collected_data["page_count"] = current_book_db_doc["page_count"]
            
            current_genres_db_doc = self.book_genres_scraped_collection.find_one({"book_id": book_id}, {"genres": 1})
            if current_genres_db_doc and current_genres_db_doc.get("genres"):
                 # I generi sono salvati come {"genre": 1}, quindi prendiamo le chiavi
                collected_data["genres"] = list(current_genres_db_doc["genres"].keys())
            self.logger.debug(f"Dati esistenti per {book_id} prima dei provider: {collected_data}")
        except PyMongoError as e:
            self.logger.error(f"Errore nel caricamento dei dati esistenti per {book_id} dal DB: {e}")


        for provider in self.providers:
            # Verifica se i dati *desiderati* sono già stati raccolti o erano già nel DB
            # I campi desiderati sono quelli che stiamo cercando di arricchire.
            # Se un campo è già in collected_data (perché era nel DB o trovato da un provider precedente)
            # e ha un valore valido, allora non serve cercarlo di nuovo.
            all_desired_fields_present = True
            desired_fields_to_check = ["description", "page_count", "genres"]
            for field in desired_fields_to_check:
                if field not in collected_data or not collected_data.get(field): # Se manca o è "falsy"
                    all_desired_fields_present = False
                    break
            
            if all_desired_fields_present:
                self.logger.info(f"Tutti i dati necessari per {book_id} già presenti o raccolti. Fermo i provider.")
                break
            
            self.logger.info(f"Tentativo con provider: {provider.get_name()} per libro '{title}'")
            attempt_log_entry = {"provider_name": provider.get_name(), "status": "NOT_FOUND", "data_retrieved": {}}
            try:
                # Passiamo una COPIA di collected_data a existing_data
                provider_data = provider.fetch_data(title, authors, existing_data=collected_data.copy() if collected_data else None)
                
                if provider_data:
                    attempt_log_entry["status"] = "SUCCESS"
                    # Logga solo i *nuovi* campi trovati dal provider
                    newly_found_data_by_provider = {
                        k: v for k, v in provider_data.items() if k not in collected_data or not collected_data.get(k) # type: ignore
                    }
                    attempt_log_entry["data_retrieved"] = dict(newly_found_data_by_provider)

                    # Unisci i dati: sovrascrivi solo se il campo non esiste in collected_data
                    # o se il valore in collected_data è "falsy" (None, 0, stringa vuota, lista vuota)
                    # e il nuovo valore da provider_data è "truthy".
                    for key, value in provider_data.items():
                        if key not in collected_data or not collected_data.get(key): # type: ignore
                            if value: # Solo se il nuovo valore è "truthy"
                                collected_data[key] = value # type: ignore
                    self.logger.info(f"Dati da {provider.get_name()}: {list(provider_data.keys())}. Dati raccolti finora: {list(collected_data.keys())}")
                else:
                    self.logger.info(f"{provider.get_name()} non ha trovato nuovi dati per '{title}'.")
                    attempt_log_entry["status"] = "NOT_FOUND"

            except Exception as e:
                self.logger.error(f"Errore con provider {provider.get_name()} per '{title}': {e}", exc_info=True)
                attempt_log_entry["status"] = "ERROR"
                attempt_log_entry["error_message"] = str(e)
            
            provider_attempts_log.append(attempt_log_entry)
            time.sleep(0.5)

        final_status: AugmentationStatus
        updated_fields_in_books: List[str] = []
        genres_flag: bool = False

        if collected_data:
            desired_fields = ["description", "page_count", "genres"]
            # Controlla quali dei campi desiderati sono effettivamente presenti e validi in collected_data
            # Questo è cruciale perché collected_data potrebbe contenere dati dal DB che non vogliamo necessariamente "ritrovare"
            # ma piuttosto campi che erano *mancanti* e sono stati *aggiunti*.
            # Tuttavia, per lo status, ci interessa se i campi desiderati sono ora popolati.
            found_and_valid_fields = [
                f for f in desired_fields 
                if collected_data.get(f) and ( # Esiste ed è truthy
                    (isinstance(collected_data[f], str) and collected_data[f].strip()) or
                    (isinstance(collected_data[f], int) and collected_data[f] > 0) or # type: ignore
                    (isinstance(collected_data[f], list) and collected_data[f])
                )
            ]

            if len(found_and_valid_fields) == len(desired_fields):
                final_status = "SUCCESS_FULL"
            elif found_and_valid_fields:
                final_status = "SUCCESS_PARTIAL"
            else:
                # Se collected_data ha qualcosa, ma nessuno dei *desiderati* è valido, è comunque NOT_FOUND
                # Questo può succedere se i dati iniziali dal DB erano incompleti e nessun provider ha aiutato.
                final_status = "NOT_FOUND" 
            
            # Aggiorna il DB solo con i dati in collected_data che potrebbero essere nuovi o aggiornati
            # _update_database si occuperà di non modificare se i valori sono identici.
            updated_fields_in_books, genres_flag = self._update_database(book_id, collected_data)

            # Se nessun campo è stato effettivamente modificato nel DB E non era già SUCCESS_FULL da log precedente
            if not updated_fields_in_books and not genres_flag and final_status != "NOT_FOUND":
                 # Se era SUCCESS_FULL o SUCCESS_PARTIAL ma nulla è cambiato,
                 # significa che i dati raccolti (o dal DB iniziale) erano già i migliori disponibili.
                 self.logger.info(f"Nessun aggiornamento effettivo al DB per {book_id}, i dati erano già presenti o i migliori disponibili.")
                 # Non cambiamo lo status qui, perché lo status riflette se *abbiamo* i dati, non se li abbiamo *modificati ora*.
        else:
            final_status = "NOT_FOUND"
        
        if any(p_log['status'] == 'ERROR' for p_log in provider_attempts_log) and final_status not in ["SUCCESS_FULL"]:
            final_status = "ERROR_PROVIDER" 

        self._log_augmentation_details(book_id, final_status, provider_attempts_log, updated_fields_in_books, genres_flag)
        self.logger.info(f"--- Fine processamento per Book ID: {book_id}. Esito: {final_status} ---")

    def run_augmentation_from_csv(self, csv_file_path: str, limit: Optional[int] = None):
        self.logger.info(f"Avvio arricchimento dati dal file CSV: {csv_file_path}")
        if not os.path.exists(csv_file_path):
            self.logger.error(f"File CSV di input non trovato: {csv_file_path}")
            return

        processed_count = 0
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if limit is not None and processed_count >= limit:
                        self.logger.info(f"Raggiunto limite di {limit} libri da processare.")
                        break
                    self.process_book_row(row)
                    processed_count += 1
                    if (processed_count % 10 == 0 and processed_count > 0) or processed_count == 1:
                        self.logger.info(f"Processati {processed_count} libri finora...")
        except FileNotFoundError: # Già gestito sopra, ma per sicurezza
            self.logger.error(f"File CSV '{csv_file_path}' non trovato durante la lettura.")
        except Exception as e:
            self.logger.critical(f"Errore critico durante la lettura o il processamento del CSV: {e}", exc_info=True)
        
        self.logger.info(f"Processo di arricchimento completato. Processati {processed_count} libri dal CSV.")