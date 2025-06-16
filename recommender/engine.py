'''
    Si connette a MongoDB per caricare i dati dei libri.

    Pre-processa i dati testuali (titolo, descrizione, generi, etc.) per creare un "profilo" vettoriale per ogni libro.

    Calcola la similarità tra tutti i libri.

    Fornisce una funzione per ottenere le raccomandazioni.
'''

# recommender/engine.py
# recommender/engine.py
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from etl.MongoDBConnection import MongoDBConnection
from utils.logger import LoggerManager

class ContentBasedRecommender:
    def __init__(self, collection_name: str = 'books'):
        """
        Inizializza il motore.
        Args:
            collection_name: Il nome della collezione principale dei libri (es. 'books' o 'books_augmented').
        """
        self.logger = LoggerManager().get_logger()
        self.mongo_conn = MongoDBConnection()
        self.collection_name = collection_name
        
        self.df_books = None
        self.cosine_sim_matrix = None
        self.book_indices = None
        
        self._load_and_prepare_data()

    def _load_and_prepare_data(self):
        """
        Carica i dati da MongoDB usando una Aggregation Pipeline per unire le collezioni,
        li prepara e pre-calcola la matrice di similarità.
        """
        self.logger.info(f"Loading and preparing data from '{self.collection_name}' for the recommender engine...")
        try:
            db = self.mongo_conn.get_database()
            
            # --- AGGREGATION PIPELINE DI MONGODB ---
            # Questa pipeline unisce le collezioni direttamente nel database, in modo molto efficiente.
            aggregation_pipeline = [
                # Fase 1: Inizia dalla collezione dei libri
                {
                    '$match': { 'description': {'$ne': ''} } # Opzionale: filtra libri senza descrizione
                },
                # Fase 2: Unisci con 'book_genres'
                {
                    '$lookup': {
                        'from': 'book_genres',
                        'localField': 'book_id',
                        'foreignField': 'book_id',
                        'as': 'genre_info'
                    }
                },
                # Fase 3: $unwind per de-normalizzare l'array 'genre_info' (solitamente contiene 1 elemento)
                {
                    '$unwind': { 'path': '$genre_info', 'preserveNullAndEmptyArrays': True }
                },
                # Fase 4: $unwind per de-normalizzare l'array degli autori
                {
                    '$unwind': { 'path': '$author_id', 'preserveNullAndEmptyArrays': True }
                },
                # Fase 5: Unisci con la collezione 'authors'
                {
                    '$lookup': {
                        'from': 'authors',
                        'localField': 'author_id.author_id',
                        'foreignField': 'author_id',
                        'as': 'author_details'
                    }
                },
                # Fase 6: $unwind per de-normalizzare i dettagli dell'autore
                {
                    '$unwind': { 'path': '$author_details', 'preserveNullAndEmptyArrays': True }
                },
                # Fase 7: Raggruppa per libro per ricomporre gli autori in una lista
                {
                    '$group': {
                        '_id': '$book_id',
                        'book_title': { '$first': '$book_title' },
                        'description': { '$first': '$description' },
                        'popular_shelves': { '$first': '$popular_shelves' },
                        'genres': { '$first': '$genre_info.genres' },
                        'authors': { '$push': '$author_details.name' } # Crea una lista di nomi di autori
                    }
                },
                # Fase 8: Proietta solo i campi che ci servono
                {
                    '$project': {
                        '_id': 0, # Escludi l'ID di default del gruppo
                        'book_id': '$_id',
                        'book_title': 1,
                        'description': 1,
                        'popular_shelves': 1,
                        'genres': 1,
                        'authors': 1
                    }
                },
                # Opzionale: Limita il numero di libri per test più veloci
                # { '$limit': 20000 }
            ]

            self.logger.info("Executing MongoDB aggregation pipeline to join collections...")
            cursor = db[self.collection_name].aggregate(aggregation_pipeline)
            
            self.df_books = pd.DataFrame(list(cursor))

            if self.df_books.empty:
                self.logger.error(f"No books found in '{self.collection_name}' after aggregation. Cannot build recommender.")
                return

            self.logger.info(f"Loaded {len(self.df_books)} processed books from the database.")

            # --- FEATURE ENGINEERING ---
            
            # 1. Pulisci i dati e gestisci valori mancanti
            self.df_books['description'] = self.df_books['description'].fillna('')
            self.df_books['book_title'] = self.df_books['book_title'].fillna('')

            # 2. Estrai tag da 'popular_shelves' e 'genres'
            def extract_tags(row):
                shelves = row['popular_shelves']
                genres = row['genres']
                authors = row['authors']
                
                tags = set()
                # Estrai nomi dagli scaffali popolari, ignorando 'to-read' e 'currently-reading'
                if isinstance(shelves, list):
                    for shelf in shelves:
                        if isinstance(shelf, dict) and 'name' in shelf and shelf['name'] not in ['to-read', 'currently-reading']:
                            tags.add(shelf['name'].replace('-', ' ')) # Sostituisci '-' con spazio
                
                # Estrai chiavi (nomi dei generi) dal dizionario genres
                if isinstance(genres, dict):
                    for genre_name in genres.keys():
                        tags.add(genre_name.replace('-', ' '))
                
                # Aggiungi i nomi degli autori
                if isinstance(authors, list):
                    for author_name in authors:
                        if author_name:
                            tags.add(author_name)

                return ' '.join(list(tags))

            self.logger.info("Engineering content features from genres, shelves, and authors...")
            self.df_books['content_tags'] = self.df_books.apply(extract_tags, axis=1)

            # 3. Combina tutto in un'unica feature "contenuto"
            # Pesiamo di più il titolo e i tag ripetendoli
            self.df_books['content'] = (self.df_books['book_title'] + ' ') * 3 + \
                                       (self.df_books['content_tags'] + ' ') * 2 + \
                                       self.df_books['description']

            # --- VETTORIZZAZIONE E CALCOLO SIMILARITÀ ---
            tfidf = TfidfVectorizer(stop_words='english', max_features=10000) # Aumentato max_features
            tfidf_matrix = tfidf.fit_transform(self.df_books['content'])

            self.logger.info("Calculating cosine similarity matrix...")
            self.cosine_sim_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)
            self.logger.info("Cosine similarity matrix calculated successfully.")

            # Mappa per trovare l'indice dal titolo del libro
            self.book_indices = pd.Series(self.df_books.index, index=self.df_books['book_title']).drop_duplicates()

        except Exception as e:
            self.logger.critical(f"Failed to initialize recommender engine: {e}", exc_info=True)
            self.df_books = None
            self.cosine_sim_matrix = None
            self.book_indices = None

    def get_recommendations(self, input_book_titles: list, top_n: int = 10) -> list:
        """
        Fornisce raccomandazioni basate su una lista di titoli di libri.
        (Questo metodo rimane invariato)
        """
        if self.cosine_sim_matrix is None or self.df_books is None:
            self.logger.error("Recommender is not ready. Data loading might have failed.")
            return []

        input_indices = []
        for title in input_book_titles:
            # Gestisce il caso in cui lo stesso titolo possa apparire più volte
            if title in self.book_indices:
                idx = self.book_indices[title]
                if isinstance(idx, pd.Series): # Se ci sono duplicati, prendi il primo
                    idx = idx.iloc[0]
                input_indices.append(idx)
            else:
                self.logger.warning(f"Book '{title}' not found in the dataset. Skipping.")

        if not input_indices:
            self.logger.error("None of the input books were found in the dataset.")
            return []
        
        self.logger.info(f"Found indices for input books: {input_indices}")

        avg_sim_scores = self.cosine_sim_matrix[input_indices].mean(axis=0)
        sim_scores_with_indices = list(enumerate(avg_sim_scores))
        sim_scores_with_indices = sorted(sim_scores_with_indices, key=lambda x: x[1], reverse=True)
        final_book_indices = [i[0] for i in sim_scores_with_indices if i[0] not in input_indices]
        top_indices = final_book_indices[1:top_n+1] # Partiamo da 1 per escludere il libro stesso (massima similarità)
        
        return self.df_books['book_title'].iloc[top_indices].tolist()