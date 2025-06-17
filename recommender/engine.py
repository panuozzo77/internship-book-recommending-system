# recommender/engine2.py
import os
from typing import List
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
# NUOVO IMPORT
from annoy import AnnoyIndex

from etl.MongoDBConnection import MongoDBConnection
from utils.logger import LoggerManager
from core.path_registry import PathRegistry # Importa per salvare l'indice

class ContentBasedAnnoyRecommender:
    def __init__(self, collection_name: str = 'books'):
        self.logger = LoggerManager().get_logger()
        self.mongo_conn = MongoDBConnection()
        self.collection_name = collection_name
        
        # --- NUOVE PROPRIETÀ ---
        self.index = None
        self.vectorizer = None
        self.book_indices_map = None # Mapping da titolo a indice intero
        self.book_titles_list = None # Mapping da indice intero a titolo
        
        self.registry = PathRegistry()
        self.processed_data_dir = self.registry.get_path('processed_datasets_dir')
        if not self.processed_data_dir:
            raise ValueError("Path 'processed_datasets_dir' non configurato in PathRegistry.")
        
        self.vector_size = 500 # Dimensione dei vettori. Ridotta per efficienza.
        self.annoy_index_path = os.path.join(self.processed_data_dir, f'books_index_{self.vector_size}d.ann')
        
        self._load_or_build_index()

    def _load_or_build_index(self):
        """
        Carica l'indice Annoy se esiste, altrimenti lo costruisce da zero.
        """
        if os.path.exists(self.annoy_index_path):
            self.logger.info(f"Found existing index. Loading from {self.annoy_index_path}")
            self.index = AnnoyIndex(self.vector_size, 'angular') # 'angular' è la distanza coseno
            self.index.load(self.annoy_index_path)

            # Carica anche i metadati necessari (vectorizer e mapping dei libri)
            # In una implementazionie reale, salveresti questi con pickle o joblib
            # Per semplicità, li ricarichiamo sempre per ora.
            self._load_metadata()
            self.logger.info("Index and metadata loaded successfully.")
        else:
            self.logger.info("No index found. Building a new one...")
            self._build_index()

    def _load_metadata(self):
        """Carica i dati dei libri e prepara i mapping, senza costruire l'indice."""
        # Questa è una versione semplificata di _load_and_prepare_data
        # per evitare di ricalcolare tutto ogni volta.
        self.logger.info("Loading book metadata for mapping...")
        db = self.mongo_conn.get_database()
        
        # Carichiamo solo gli ID e i titoli per il mapping
        cursor = db[self.collection_name].find({}, {'_id': 0, 'book_id': 1, 'book_title': 1})
        df_meta = pd.DataFrame(list(cursor))
        
        # Semplice mapping basato sull'ordine del DataFrame (DEVE essere consistente)
        df_meta = df_meta.sort_values(by='book_id').reset_index(drop=True)
        self.book_titles_list = df_meta['book_title'].tolist()
        self.book_indices_map = pd.Series(df_meta.index, index=df_meta['book_title']).drop_duplicates()
        self.logger.info(f"Metadata for {len(self.book_titles_list)} books loaded.")


    def _build_index(self):
        """
        Esegue la pipeline di preparazione dati e costruisce l'indice Annoy.
        """
        # Questa funzione ora contiene la logica di preparazione dati
        # e la costruzione dell'indice.
        
        # --- FASE 1: Preparazione dati (come prima, ma senza Pandas) ---
        self.logger.info("Loading and preparing data to build index...")
        db = self.mongo_conn.get_database()
        # Qui potresti usare la stessa aggregation pipeline di prima per unire i dati
        # Per semplicità qui mostro il caricamento e la preparazione
        
        # Carica tutti i dati necessari per creare il 'content'
        # NOTA: Questa parte è ancora memory-intensive. Potrebbe essere ottimizzata
        # processando i documenti in batch.
        # ... (stessa aggregation pipeline di prima per ottenere il DataFrame) ...
        # ... (stesso codice per creare la colonna 'content') ...
        # Per ora assumiamo di avere il DataFrame df_books come prima
        
        # Esempio semplificato di caricamento
        # (Usa la tua pipeline di aggregazione completa qui)
        cursor = db[self.collection_name].find({}, {'book_id': 1, 'book_title': 1, 'description': 1})
        df_books = pd.DataFrame(list(cursor)).sort_values(by='book_id').reset_index(drop=True)
        df_books['content'] = df_books['book_title'].fillna('') + ' ' + df_books['description'].fillna('')
        
        # --- FASE 2: Vettorizzazione ---
        self.logger.info(f"Vectorizing content into {self.vector_size}-dimensional space...")
        self.vectorizer = TfidfVectorizer(max_features=self.vector_size, stop_words='english')
        tfidf_matrix = self.vectorizer.fit_transform(df_books['content'])
        
        # --- FASE 3: Costruzione Indice Annoy ---
        self.logger.info("Building Annoy index...")
        self.index = AnnoyIndex(self.vector_size, 'angular')
        
        for i in range(tfidf_matrix.shape[0]):
            vector = tfidf_matrix[i].toarray()[0]
            self.index.add_item(i, vector)
            if (i + 1) % 100000 == 0:
                self.logger.info(f"Added {i+1}/{tfidf_matrix.shape[0]} items to index...")

        # Costruisce l'indice (il numero di alberi è un trade-off tra precisione e velocità/dimensione)
        self.index.build(50) # 50 alberi
        self.logger.info("Index built. Saving to disk...")
        self.index.save(self.annoy_index_path)
        self.logger.info(f"Index saved to {self.annoy_index_path}")

        # SALVA I MAPPING dopo la costruzione
        self.book_titles_list = df_books['book_title'].tolist()
        self.book_indices_map = pd.Series(df_books.index, index=df_books['book_title']).drop_duplicates()

    def get_recommendations(self, input_book_titles: list, top_n: int = 10) -> list:
        if self.index is None:
            self.logger.error("Recommender is not ready.")
            return []

        input_indices = []
        for title in input_book_titles:
            if title in self.book_indices_map:
                idx = self.book_indices_map[title]
                if isinstance(idx, pd.Series): idx = idx.iloc[0]
                input_indices.append(idx)
            else:
                self.logger.warning(f"Book '{title}' not found. Skipping.")

        if not input_indices:
            return []

        # Ottieni i vettori per i libri di input DALL'INDICE
        input_vectors = [self.index.get_item_vector(i) for i in input_indices]
        
        # Calcola il profilo medio
        import numpy as np
        profile_vector = np.mean(input_vectors, axis=0)

        # Cerca i vicini più prossimi al profilo medio
        # Il primo risultato è il profilo stesso, quindi chiediamo N+len(input) items
        # include_distances=False restituisce solo gli indici
        result_indices = self.index.get_nns_by_vector(profile_vector, top_n + len(input_indices))
        
        # Filtra i libri che l'utente ha già fornito
        final_indices = [idx for idx in result_indices if idx not in input_indices]
        
        # Prendi i primi N e restituisci i titoli
        return [self.book_titles_list[i] for i in final_indices[:top_n]]
    
    def get_recommendations_by_profile(self, profile_vector: np.ndarray, exclude_indices: set, top_n: int = 10) -> List[str]:
        """
        Trova i libri più simili a un dato profilo vettoriale, escludendo alcuni indici.
        
        Args:
            profile_vector: Il vettore numpy che rappresenta il profilo dell'utente.
            exclude_indices: Un set di indici di libri da escludere dai risultati.
            top_n: Il numero di raccomandazioni da restituire.

        Returns:
            Una lista di titoli di libri raccomandati.
        """
        if self.index is None:
            self.logger.error("Recommender non è pronto (indice non caricato).")
            return []
            
        # Cerca i vicini più prossimi usando Annoy
        # Chiediamo più risultati per avere margine dopo il filtraggio
        num_candidates = top_n + len(exclude_indices)
        result_indices = self.index.get_nns_by_vector(profile_vector, num_candidates)
        
        # Filtra i libri da escludere
        final_indices = [idx for idx in result_indices if idx not in exclude_indices]
        
        # Prendi i primi N e restituisci i loro titoli
        return [self.book_titles_list[i] for i in final_indices[:top_n]]