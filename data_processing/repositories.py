# data_processing/repositories.py

import datetime
import re
import logging
from typing import Optional, Dict, Any, List

from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult, UpdateResult

from core.utils.LoggerManager import LoggerManager

class MongoBookRepository:
    """
    Gestisce tutte le operazioni di accesso al database per le entità libro, autore e serie.
    Questa classe è l'unico punto che "conosce" la struttura di MongoDB.
    """
    def __init__(self, db: Database):
        self.db = db
        self.logger: logging.Logger = LoggerManager().get_logger()
        self.counters_collection = self.db.system_counters
        self.books_collection = self.db.books
        self.authors_collection = self.db.authors
        self.series_collection = self.db.book_series
        self.genres_collection = self.db.book_genres
        self.adder_log_collection = self.db.book_addition_log
        self.updater_log_collection = self.db.book_update_log

    def _get_next_sequence_value(self, sequence_name: str) -> str:
        """Genera un ID progressivo stringa (es. 'add_book_124')."""
        counter = self.counters_collection.find_one_and_update(
            {'_id': sequence_name},
            {'$inc': {'sequence_value': 1}},
            upsert=True,
            return_document=True
        )
        prefix = sequence_name.replace('_id', '')
        return f"add_{prefix}_{counter['sequence_value']}"

    # --- Metodi di Lettura ---
    def find_book_by_id(self, book_id: str) -> Optional[Dict[str, Any]]:
        return self.books_collection.find_one({"book_id": book_id})

    def find_book_by_title_author(self, title: str, author_name: str) -> Optional[Dict[str, Any]]:
        author_doc = self.find_author_by_name(author_name)
        if not author_doc:
            return None
        
        author_id_to_find = author_doc["author_id"]
        
        return self.books_collection.find_one({
            "book_title": {"$regex": f"^{re.escape(title)}$", "$options": "i"},
            "author_id": {
                "$elemMatch": {"author_id": author_id_to_find}
            }
        })
        
    def find_author_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        return self.authors_collection.find_one({"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
        
    def find_series_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        return self.series_collection.find_one({"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})

    # --- Metodi di Scrittura/Aggiornamento ---
    def get_or_create_author(self, author_name: str) -> Optional[str]:
        author = self.find_author_by_name(author_name)
        if author:
            return author["author_id"]
        
        new_id = self._get_next_sequence_value("author_id")
        new_doc = {"author_id": new_id, "name": author_name.strip(), "ratings_count": 0, "text_reviews_count": 0}
        self.authors_collection.insert_one(new_doc)
        self.logger.info(f"Nuovo autore '{author_name}' creato con ID: {new_id}")
        return new_id

    def get_or_create_series(self, series_data: Optional[Dict[str, Any]]) -> Optional[str]:
        if not series_data or not series_data.get("name"):
            return None
        
        series_name = series_data["name"]
        series = self.find_series_by_name(series_name)
        if series:
            return series["series_id"]

        new_id = self._get_next_sequence_value("series_id")
        new_doc = {
            "series_id": new_id,
            "name": series_name.strip(),
            "description": series_data.get("description", ""),
            "primary_work_count": "1",
            "series_works_count": "1"
        }
        self.series_collection.insert_one(new_doc)
        self.logger.info(f"Nuova serie '{series_name}' creata con ID: {new_id}")
        return new_id

    def add_book(self, book_doc: Dict[str, Any]) -> InsertOneResult:
        book_doc["book_id"] = self._get_next_sequence_value("book_id")
        return self.books_collection.insert_one(book_doc)

    def add_genres(self, book_id: str, genres: Dict[str, int]) -> InsertOneResult:
        return self.genres_collection.insert_one({"book_id": book_id, "genres": genres})

    def update_book(self, book_id: str, update_payload: Dict[str, Any]) -> UpdateResult:
        return self.books_collection.update_one({"book_id": book_id}, {"$set": update_payload})

    def log_operation(self, collection_name: str, status: str, details: Dict[str, Any]):
        log_collections = {
            "addition": self.adder_log_collection,
            "update": self.updater_log_collection
        }
        #log_entry = {"status": status, "timestamp": datetime.now(timezone.utc), **details}
        log_entry = {"status": status, "timestamp": datetime.datetime.now(datetime.timezone.utc), **details}
        log_collections[collection_name].insert_one(log_entry)