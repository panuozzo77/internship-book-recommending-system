# recommender/repository.py
import pandas as pd
from typing import Any
from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager

class BookRepository:
    """
    Responsabile del caricamento dei dati dei libri da MongoDB.
    """
    def __init__(self, db_connection: MongoDBConnection, collection_name: str = 'books'):
        self.db = db_connection.get_database()
        self.collection_name = collection_name
        self.logger = LoggerManager().get_logger()

    def fetch_all_books_for_indexing(self) -> pd.DataFrame:
        """
        Carica i dati necessari per costruire l'indice di raccomandazione.
        """
        self.logger.info(f"Fetching books from collection '{self.collection_name}'...")
        query = {}
        projection = {
            '_id': 0, 
            'book_id': 1, 
            'book_title': 1, 
            'description': 1, 
            'page_count': 1
        }
        
        cursor = self.db[self.collection_name].find(query, projection)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            self.logger.warning("No books found in the database.")
            return pd.DataFrame()

        self.logger.info(f"Successfully fetched {len(df)} books.")
        return df
    
class UserInteractionRepository:
    """
    Responsabile del caricamento dei dati di interazione utente-libro da MongoDB.
    """
    def __init__(self, db_connection: MongoDBConnection, collection_name: str = 'reviews'):
        self.db = db_connection.get_database()
        self.collection_name = collection_name
        self.logger = LoggerManager().get_logger()

    def find_interactions_by_user(self, user_id: Any) -> pd.DataFrame:
        """
        Trova tutti i libri con cui un utente ha interagito.
        """
        self.logger.info(f"Fetching interactions for user_id '{user_id}'...")
        # Uniamo con la collection 'books' per ottenere direttamente i titoli
        pipeline = [
            { '$match': { 'user_id': user_id } },
            {
                '$lookup': {
                    'from': 'books',
                    'localField': 'book_id',
                    'foreignField': 'book_id',
                    'as': 'book_details'
                }
            },
            { '$unwind': '$book_details' },
            {
                '$project': {
                    '_id': 0,
                    'user_id': 1,
                    'book_id': 1,
                    'book_title': '$book_details.book_title',
                    'page_count': '$book_details.page_count', # Ci servirà per il re-ranking
                    'rating': '$rating',  # Aggiungiamo il rating se disponibile
                }
            }
        ]
        
        cursor = self.db[self.collection_name].aggregate(pipeline)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            self.logger.warning(f"No interactions found for user_id '{user_id}'.")
        
        return df