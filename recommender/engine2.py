# recommender/engine.py
import os
from typing import List
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
# NUOVO IMPORT
from annoy import AnnoyIndex

from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager
from core.PathRegistry import PathRegistry # Importa per salvare l'indice


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
        self.df_books = None # DataFrame che conterrà i metadati
        
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
        cursor = db[self.collection_name].find({}, {'_id': 0, 'book_id': 1, 'book_title': 1, "page_count": 1})
        df_meta = pd.DataFrame(list(cursor))
        df_meta['page_count'] = pd.to_numeric(df_meta['page_count'], errors='coerce')
        '''
        # Semplice mapping basato sull'ordine del DataFrame (DEVE essere consistente)
        df_meta = df_meta.sort_values(by='book_id').reset_index(drop=True)
        self.book_titles_list = df_meta['book_title'].tolist()
        self.book_indices_map = pd.Series(df_meta.index, index=df_meta['book_title']).drop_duplicates()
        self.logger.info(f"Metadata for {len(self.book_titles_list)} books loaded.")
        '''
        self.df_books = df_meta.sort_values(by='book_id').reset_index(drop=True)
        self.book_titles_list = self.df_books['book_title'].tolist()
        self.book_indices_map = pd.Series(self.df_books.index, index=self.df_books['book_title']).drop_duplicates()
        
        self.logger.info(f"Metadata for {len(self.book_titles_list)} books loaded.")


    def _build_index(self):
        """Esegue la pipeline di preparazione dati e costruisce l'indice Annoy."""
        self.logger.info("Loading and preparing data to build index...")
        db = self.mongo_conn.get_database()
        
        # Per ora, usiamo una query find semplificata per il test
        cursor = db[self.collection_name].find({}, {'book_id': 1, 'book_title': 1, 'description': 1, 'page_count': 1})
        self.df_books = pd.DataFrame(list(cursor)).sort_values(by='book_id').reset_index(drop=True)
        
        # Pulisci i dati come in _load_metadata
        self.df_books['page_count'] = pd.to_numeric(self.df_books['page_count'], errors='coerce')
        
        self.df_books['content'] = self.df_books['book_title'].fillna('') + ' ' + self.df_books['description'].fillna('')

        # --- FASE 2: Vettorizzazione ---
        self.logger.info(f"Vectorizing content into {self.vector_size}-dimensional space...")
        self.vectorizer = TfidfVectorizer(max_features=self.vector_size, stop_words='english')
        tfidf_matrix = self.vectorizer.fit_transform(self.df_books['content'])
        
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
        self.book_titles_list = self.df_books['book_title'].tolist()
        self.book_indices_map = pd.Series(self.df_books.index, index=self.df_books['book_title']).drop_duplicates()
    

    def get_recommendations_by_profile(self, profile_vector: np.ndarray, exclude_indices: set, top_n: int = 10, avg_page_count: float = 0.0) -> List[str]:
        """
        Trova e riordina i libri più simili a un profilo, considerando la lunghezza preferita.
        
        Args:
            profile_vector: Il vettore numpy che rappresenta il profilo dell'utente.
            exclude_indices: Un set di indici di libri da escludere.
            top_n: Il numero di raccomandazioni da restituire.
            avg_page_count (opzionale): La lunghezza media dei libri preferiti dall'utente.
        """
        if self.index is None:
            self.logger.error("Recommender non è pronto (indice non caricato).")
            return []
            
        # Fase 1: Genera candidati con Annoy
        num_candidates = top_n * 20 # Prendiamo molti più candidati del necessario
        search_results = self.index.get_nns_by_vector(profile_vector, num_candidates, include_distances=True)
        
        candidate_indices = search_results[0]
        distances = search_results[1]
        
        # Fase 2: Filtra e riordina i candidati
        final_scores = []
        
        # Calcola i limiti di pagine accettabili
        lower_bound = avg_page_count * 0.8
        upper_bound = avg_page_count * 1.2
        
        for i, book_idx in enumerate(candidate_indices):
            if book_idx in exclude_indices:
                continue

            # Punteggio base dalla similarità del contenuto
            similarity_score = 1 - (distances[i]**2) / 2 # Conversione da distanza angolare a similarità

            # Calcolo del punteggio bonus basato sulla lunghezza
            page_bonus = 0.0
            if avg_page_count > 0: # Applica il bonus solo se abbiamo una media valida
                try:
                    book_page_count = self.df_books.iloc[book_idx]['page_count']
                    # pd.notna gestisce correttamente i NaN
                    if pd.notna(book_page_count) and book_page_count > 0:
                        
                        # Se rientra nel range ±20%, diamo un bonus.
                        if lower_bound <= book_page_count <= upper_bound:
                            # Formula per dare un bonus inversamente proporzionale alla differenza
                            # Più è vicino alla media, maggiore è il bonus.
                            diff_ratio = abs(book_page_count - avg_page_count) / avg_page_count
                            page_bonus = 0.25 * (1 - diff_ratio) # Bonus max di 0.25
                except (KeyError, IndexError):
                    pass # Ignora errori se l'indice non è valido

            final_score = similarity_score + page_bonus
            final_scores.append((book_idx, final_score))

        # Ordina per il punteggio finale
        final_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Estrai gli indici finali
        final_book_indices = [idx for idx, score in final_scores]
        
        # Prendi i primi N e restituisci i loro titoli
        return [self.book_titles_list[i] for i in final_book_indices[:top_n]]

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
    