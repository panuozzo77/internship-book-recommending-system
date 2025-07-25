# recommender/repository.py
import pandas as pd
from typing import Any, List, Set, Optional
from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager
from werkzeug.security import generate_password_hash, check_password_hash

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
        top_shelves = {doc['_id'] for doc in cursor} if cursor else set()
        self.logger.info(f"Found {len(top_shelves)} unique top shelves.")
        return top_shelves
    
    def get_book_details_by_id(self, book_id: str) -> Optional[dict]:
        """
        Retrieves detailed information for a single book, including author and series.
        """
        self.logger.info(f"Fetching details for book_id '{book_id}'...")
        pipeline = [
            {'$match': {'book_id': book_id}},
            {
                '$lookup': {
                    'from': 'authors',
                    'localField': 'author_id.author_id',
                    'foreignField': 'author_id',
                    'as': 'author_details'
                }
            },
            {
                '$lookup': {
                    'from': 'series',
                    'localField': 'series.series_id',
                    'foreignField': 'series_id',
                    'as': 'series_details'
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'book_id': 1,
                    'book_title': 1,
                    'description': 1,
                    'author_names': '$author_details.name',
                    'series_names': '$series_details.name'
                }
            }
        ]
        
        cursor = self.db['books'].aggregate(pipeline)
        result = list(cursor)
        
        if not result:
            self.logger.warning(f"No details found for book_id '{book_id}'.")
            return None
            
        return result[0]
    
    def get_book_id_by_title(self, book_title: str) -> Optional[str]:
        """
        Retrieves the book_id for a given book_title.
        """
        self.logger.info(f"Fetching book_id for title '{book_title}'...")
        result = self.db[self.collection_name].find_one(
            {'book_title': book_title},
            {'_id': 0, 'book_id': 1}
        )
        if result and 'book_id' in result:
            return result['book_id']
        
        self.logger.warning(f"No book found with title '{book_title}'.")
        return None

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
            # --- NUOVI STADI PER ARRICCHIRE CON I GENERI ---
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
            # De-normalizziamo i risultati per averli a disposizione
            {'$unwind': {'path': '$book_genres_data', 'preserveNullAndEmptyArrays': True}},
            {'$unwind': {'path': '$scraped_genres_data', 'preserveNullAndEmptyArrays': True}},
            {
                '$project': {
                    '_id': 0,
                    'user_id': 1,
                    'book_id': 1,
                    'book_title': '$book_details.book_title',
                    'rating': '$rating',
                    'page_count': '$book_details.page_count', # Aggiunto per il re-ranking
                    # Includiamo i dati di genere nel risultato finale
                    'genres': '$book_genres_data.genres',
                    'scraped_genres': '$scraped_genres_data.genres'
                }
            }
        ]
        '''
        [
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
        '''
        
        cursor = self.db[self.collection_name].aggregate(pipeline)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            self.logger.warning(f"No interactions found for user_id '{user_id}'.")
        
        return df

class UserRepository:
    """
    Manages user data persistence in MongoDB, including authentication.
    """
    def __init__(self, db_connection: MongoDBConnection, collection_name: str = 'users'):
        self.db = db_connection.get_database()
        self.collection = self.db[collection_name]
        self.logger = LoggerManager().get_logger()

    def create_user(self, username: str, password: str) -> Optional[Any]:
        """
        Creates a new user with a hashed password.
        Returns the new user's ID if successful, otherwise None.
        """
        if self.find_user_by_username(username):
            self.logger.warning(f"User '{username}' already exists.")
            return None
        
        hashed_password = generate_password_hash(password)
        user_data = {
            'username': username,
            'password': hashed_password
        }
        result = self.collection.insert_one(user_data)
        self.logger.info(f"User '{username}' created successfully.")
        return result.inserted_id

    def find_user_by_username(self, username: str) -> Optional[dict]:
        """Finds a user by their username."""
        return self.collection.find_one({'username': username})

    def check_password(self, username: str, password: str) -> bool:
        """Checks if the provided password is correct for the given username."""
        user = self.find_user_by_username(username)
        if user and check_password_hash(user['password'], password):
            return True
        return False
    