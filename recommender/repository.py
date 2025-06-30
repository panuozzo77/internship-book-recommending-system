# recommender/repository.py
import pandas as pd
from typing import Any, List, Set
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
        
    def get_all_books_with_related_data(self) -> List[dict]:
        """
        Carica tutti i libri e li arricchisce con i dati dei generi
        da 'book_genres' e 'book_genres_scraped' usando un'unica pipeline di aggregazione.
        """
        self.logger.info("Fetching books and joining with genre collections...")
        pipeline = [
            {
                '$lookup': {
                    'from': 'book_genres',
                    'localField': 'book_id',
                    'foreignField': 'book_id',
                    'as': 'book_genres_data'
                }
            },
            {
                '$lookup': {
                    'from': 'book_genres_scraped',
                    'localField': 'book_id',
                    'foreignField': 'book_id',
                    'as': 'scraped_genres_data'
                }
            },
            # "$unwind" per de-normalizzare i risultati dei lookup.
            # usiamo preserveNullAndEmptyArrays per non perdere libri senza generi.
            {'$unwind': {'path': '$book_genres_data', 'preserveNullAndEmptyArrays': True}},
            {'$unwind': {'path': '$scraped_genres_data', 'preserveNullAndEmptyArrays': True}},
            {
                '$project': {
                    '_id': 0,
                    'book_id': 1,
                    'book_title': '$book_title', # Usiamo 'title' come da mapping
                    'description': 1,
                    'page_count': '$num_pages', # Usiamo 'num_pages' come da mapping
                    'popular_shelves': 1,
                    'genres': '$book_genres_data.genres',
                    'scraped_genres': '$scraped_genres_data.genres'
                }
            }
        ]
        
        cursor = self.db['books'].aggregate(pipeline)
        results = list(cursor)
        self.logger.info(f"Successfully fetched and aggregated data for {len(results)} books.")
        return results

    def get_top_popular_shelves(self, limit: int = 1000) -> Set[str]:
        """
        Esegue un'aggregazione per trovare i nomi dei "popular shelves" più comuni
        in tutta la collection.
        """
        self.logger.info(f"Fetching top {limit} popular shelves...")
        pipeline = [
            {'$unwind': '$popular_shelves'},
            {
                '$addFields': {
                    'shelf_count_int': {
                        '$toInt': '$popular_shelves.count'
                    }
                }
            },
            {
                '$group': {
                    '_id': '$popular_shelves.name',
                    'total_count': {'$sum': '$shelf_count_int'}
                }
            },
            {'$sort': {'total_count': -1}},
            {'$limit': limit},
            {'$project': {'_id': 1}}
        ]
        
        cursor = self.db['books'].aggregate(pipeline)
        top_shelves = {doc['_id'] for doc in cursor}
        self.logger.info(f"Found {len(top_shelves)} unique top shelves.")
        return top_shelves
    
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