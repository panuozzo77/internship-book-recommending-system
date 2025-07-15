# recommender/model.py
import os
import joblib
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Protocol

from annoy import AnnoyIndex
from sklearn.feature_extraction.text import TfidfVectorizer

from recommender import config 
from core.PathRegistry import PathRegistry
from core.utils.LoggerManager import LoggerManager
from etl.MongoDBConnection import MongoDBConnection

# 1. Definiamo il contenitore per gli artefatti del modello
@dataclass
class RecommenderModel:
    """Contiene tutti gli artefatti necessari per le raccomandazioni."""
    vector_size: int
    vectorizer: TfidfVectorizer
    index: AnnoyIndex
    book_metadata: pd.DataFrame  # Contiene titoli, page_count, ecc. indicizzati per int
    title_to_idx: Dict[str, int]
    idx_to_title: Dict[int, str]

# 2. Classe per la costruzione del modello
class ModelBuilder:
    """Costruisce il RecommenderModel partendo da un DataFrame di libri."""
    
    def __init__(self, max_vector_size: int = config.VECTOR_SIZE, n_trees: int = config.ANNOY_N_TREES):
        self.max_vector_size = max_vector_size
        self.n_trees = n_trees
        self.logger = LoggerManager().get_logger()

    def build(self, df: pd.DataFrame) -> Optional[RecommenderModel]:
        """
        Esegue l'intera pipeline di build: pulizia, vettorizzazione e indicizzazione.
        """
        if df.empty:
            self.logger.error("Cannot build model from an empty DataFrame.")
            return None

        # --- Fase 1: Preparazione Dati ---
        self.logger.info("Preparing data for model building...")
        df_processed = self._prepare_data(df)
        
        # --- Fase 2: Vettorizzazione ---
        self.logger.info(f"Vectorizing content into {self.max_vector_size}-dimensional space...")
        vectorizer = TfidfVectorizer(max_features=self.max_vector_size, stop_words='english')
        tfidf_matrix = vectorizer.fit_transform(df_processed['content'])

        actual_vector_size = tfidf_matrix.shape[1]
        self.logger.info(f"Actual vector size after TF-IDF processing: {actual_vector_size}")
        if actual_vector_size == 0:
            self.logger.error("Vectorization resulted in 0 features. Cannot build Annoy index. Check input content.")
            return None
        
        # --- Fase 3: Costruzione Indice Annoy ---
        self.logger.info("Building Annoy index...")
        annoy_index = AnnoyIndex(actual_vector_size, config.ANNOY_METRIC)
        for i in range(tfidf_matrix.shape[0]):
            vector = tfidf_matrix[i].toarray()[0]
            annoy_index.add_item(i, vector)
        
        annoy_index.build(self.n_trees)
        self.logger.info(f"Annoy index built with {self.n_trees} trees.")
        
        # --- Fase 4: Creazione Mappe ---
        title_to_idx = pd.Series(df_processed.index, index=df_processed['book_title']).to_dict()
        idx_to_title = pd.Series(df_processed['book_title'], index=df_processed.index).to_dict()

        return RecommenderModel(
            vector_size=actual_vector_size,
            vectorizer=vectorizer,
            index=annoy_index,
            book_metadata=df_processed[['book_title', 'page_count', 'key_genres']],
            title_to_idx=title_to_idx,
            idx_to_title=idx_to_title
        )

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pulisce e prepara il DataFrame per l'indicizzazione."""
        '''
        df_copy = df.copy()
        df_copy = df_copy.sort_values(by='book_id').reset_index(drop=True)
        df_copy['page_count'] = pd.to_numeric(df_copy['page_count'], errors='coerce')
        df_copy['content'] = df_copy['book_title'].fillna('') + ' ' + df_copy['description'].fillna('')
        df_copy.drop_duplicates(subset=['book_title'], inplace=True)
        return df_copy.reset_index(drop=True)
        '''
        """
        Pulisce e prepara il DataFrame. La colonna 'content' è già fornita.
        """
        df_copy = df.copy()
        # Assicura che non ci siano ID o titoli duplicati/mancanti
        df_copy.dropna(subset=['book_id', 'book_title'], inplace=True)
        df_copy.drop_duplicates(subset=['book_id'], inplace=True)
        
        df_copy = df_copy.sort_values(by='book_id').reset_index(drop=True)
        df_copy['page_count'] = pd.to_numeric(df_copy['page_count'], errors='coerce')

        if 'key_genres' not in df_copy.columns:
            df_copy['key_genres'] = [[] for _ in range(len(df_copy))]
        else:
            # Assicura che i valori nulli siano liste vuote
            df_copy['key_genres'] = df_copy['key_genres'].apply(lambda x: x if isinstance(x, list) else [])
        
        # La colonna 'content' non viene più creata qui! È già presente.
        #df_copy.drop_duplicates(subset=['book_title'], inplace=True)
        return df_copy.reset_index(drop=True)

@dataclass
class _SerializableModelData:
    """Struttura dati per la persistenza del modello, senza l'oggetto Annoy."""
    vector_size: int
    vectorizer: TfidfVectorizer
    book_metadata: pd.DataFrame
    title_to_idx: Dict[str, int]
    idx_to_title: Dict[int, str]
    index_path: str  # Salviamo il percorso dell'indice, non l'oggetto

# 3. Classe per la persistenza
class ModelPersister:
    """Gestisce il salvataggio e il caricamento del RecommenderModel."""

    def __init__(self, path_registry: PathRegistry):
        self.processed_data_dir = path_registry.get_path(config.MODEL_ARTIFACTS_DIR_KEY)
        if not self.processed_data_dir:
            raise ValueError(f"Path '{config.MODEL_ARTIFACTS_DIR_KEY}' non configurato.")
        os.makedirs(self.processed_data_dir, exist_ok=True)
        self.logger = LoggerManager().get_logger()

    def _get_model_filepath(self, version: str = "1.0") -> str:
        """Costruisce il percorso completo del file del modello."""
        filename = config.MODEL_FILENAME_TEMPLATE.format(version=version)
        return os.path.join(self.processed_data_dir, filename)

    def save(self, model: RecommenderModel, version: str = "1.0"):
        """Salva l'intero oggetto RecommenderModel."""
        filepath = self._get_model_filepath(version)
        annoy_path = filepath.replace('.joblib', '.ann')
        
        self.logger.info(f"Saving model artifacts to {filepath} and {annoy_path}")
        
        # Annoy ha il suo metodo di salvataggio
        model.index.save(annoy_path)
        
        # Per evitare problemi, non salviamo l'oggetto Annoy con joblib.
        # Lo impostiamo a None e salviamo il percorso.
        model_to_save = _SerializableModelData(
            vector_size=model.vector_size,
            vectorizer=model.vectorizer,
            book_metadata=model.book_metadata,
            title_to_idx=model.title_to_idx,
            idx_to_title=model.idx_to_title,
            index_path=annoy_path
        )
        
        joblib.dump(model_to_save, filepath)
        self.logger.info("Model saved successfully.")

    def load(self, version: str = "1.0") -> Optional[RecommenderModel]:
        """Carica un RecommenderModel dal disco."""
        filepath = self._get_model_filepath(version)
        if not os.path.exists(filepath):
            self.logger.warning(f"Model file not found at {filepath}")
            return None
            
        self.logger.info(f"Loading model from {filepath}")
        loaded_data: _SerializableModelData = joblib.load(filepath)
        
        annoy_path = loaded_data.index_path
        if not os.path.exists(annoy_path):
            self.logger.error(f"Annoy index file not found at {annoy_path}")
            return None
            
        annoy_index = AnnoyIndex(loaded_data.vector_size, config.ANNOY_METRIC)
        annoy_index.load(annoy_path)
        self.logger.info("Annoy index loaded successfully.")
        
        return RecommenderModel(
            vector_size=loaded_data.vector_size,
            vectorizer=loaded_data.vectorizer,
            index=annoy_index,
            book_metadata=loaded_data.book_metadata,
            title_to_idx=loaded_data.title_to_idx,
            idx_to_title=loaded_data.idx_to_title
        )