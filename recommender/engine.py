# recommender/engine.py
from typing import List, Protocol, Tuple, Set
import numpy as np
import pandas as pd

from recommender.model import RecommenderModel
from recommender import config
from core.utils.LoggerManager import LoggerManager

# --- Strategy Pattern per il Re-ranking (OCP) ---
class ReRanker(Protocol):
    """
    Protocollo per le strategie di re-ranking.
    Una strategia riceve candidati e i loro punteggi e restituisce una versione ri-ordinata.
    """
    def rerank(
        self,
        candidates: List[Tuple[int, float]], # (book_idx, similarity_score)
        model: RecommenderModel,
        context: dict
    ) -> List[Tuple[int, float]]:
        ...

class GenrePreferenceReRanker:
    """
    Ri-ordina i candidati premiando i libri con generi preferiti dall'utente
    e penalizzando quelli con generi non graditi.
    """
    def rerank(
        self,
        candidates: List[Tuple[int, float]],
        model: RecommenderModel,
        context: dict
    ) -> List[Tuple[int, float]]:
        preferred_genres = context.get('preferred_genres', set())
        disliked_genres = context.get('disliked_genres', set())
        
        # Se non abbiamo preferenze sui generi, non facciamo nulla
        if not preferred_genres and not disliked_genres:
            return candidates

        reranked_scores = []
        for book_idx, similarity_score in candidates:
            genre_bonus = 0.0
            
            # Recupera i generi del libro candidato dai metadati
            book_genres = set(model.book_metadata.iloc[book_idx]['key_genres'])
            
            if book_genres:
                # Calcola l'intersezione con i generi preferiti e non graditi
                num_preferred_matches = len(book_genres.intersection(preferred_genres))
                num_disliked_matches = len(book_genres.intersection(disliked_genres))
                
                # Applica bonus e malus
                genre_bonus = (num_preferred_matches - num_disliked_matches) * config.GENRE_PREFERENCE_BONUS_WEIGHT

            final_score = similarity_score + genre_bonus
            reranked_scores.append((book_idx, final_score))
            
        # Ordina in base al nuovo punteggio finale
        reranked_scores.sort(key=lambda x: x[1], reverse=True)
        return reranked_scores

class PageCountReRanker:
    """
    Ri-ordina i candidati per premiare i libri con un numero di pagine
    simile alla media dei libri preferiti dall'utente.
    """
    def rerank(
        self,
        candidates: List[Tuple[int, float]],
        model: RecommenderModel,
        context: dict
    ) -> List[Tuple[int, float]]:
        avg_page_count = context.get('avg_page_count', 0.0)
        if not avg_page_count or avg_page_count <= 0:
            return candidates # Nessun re-ranking se non c'è una media valida

        lower_bound = avg_page_count * config.PAGE_COUNT_LOWER_BOUND_FACTOR
        upper_bound = avg_page_count * config.PAGE_COUNT_UPPER_BOUND_FACTOR
        
        reranked_scores = []
        for book_idx, similarity_score in candidates:
            page_bonus = 0.0
            book_page_count = model.book_metadata.iloc[book_idx]['page_count']
            
            if pd.notna(book_page_count) and lower_bound <= book_page_count <= upper_bound:
                # Bonus inversamente proporzionale alla distanza dalla media
                diff_ratio = abs(book_page_count - avg_page_count) / avg_page_count
                page_bonus = config.PAGE_COUNT_BONUS_WEIGHT * (1 - diff_ratio)

            final_score = similarity_score + page_bonus
            reranked_scores.append((book_idx, final_score))
            
        # Ordina in base al nuovo punteggio finale
        reranked_scores.sort(key=lambda x: x[1], reverse=True)
        return reranked_scores


# --- La classe Recommender pulita ---
class ContentBasedRecommender:
    """
    Genera raccomandazioni usando un modello pre-addestrato.
    È leggera e non ha dipendenze da I/O.
    """
    def __init__(self, model: RecommenderModel, rerankers: List[ReRanker] = None):
        if not isinstance(model, RecommenderModel):
            raise TypeError("model must be an instance of RecommenderModel")
        
        self.model = model
        self.rerankers = rerankers or []
        self.logger = LoggerManager().get_logger()

    def get_recommendations_by_titles(
        self, 
        book_titles: List[str], 
        top_n: int = 10
    ) -> List[str]:
        """
        Raccomanda libri simili a una lista di titoli forniti.
        """
        input_indices = self._get_indices_from_titles(book_titles)
        if not input_indices:
            return []
            
        # Calcola il profilo medio dai vettori dei libri di input
        input_vectors = [self.model.index.get_item_vector(i) for i in input_indices]
        profile_vector = np.mean(input_vectors, axis=0)
        
        # Usa il metodo più generico per ottenere le raccomandazioni
        # Escludiamo i libri di input e non applichiamo re-ranking specifici
        return self.get_recommendations_by_profile(
            profile_vector=profile_vector,
            exclude_indices=set(input_indices),
            top_n=top_n
        )

    def get_recommendations_by_profile(
        self, 
        profile_vector: np.ndarray, 
        exclude_indices: Set[int], 
        top_n: int = 10,
        rerank_context: dict = None
    ) -> List[str]:
        """
        Trova i libri più simili a un profilo vettoriale, applicando filtri e re-ranking.
        """
        # Fase 1: Genera candidati con Annoy (ne chiediamo di più per il re-ranking)
        num_candidates = top_n * 20
        indices, distances = self.model.index.get_nns_by_vector(
            profile_vector, num_candidates, include_distances=True
        )
        
        # Fase 2: Filtra e calcola i punteggi di similarità iniziali
        candidates = []
        for i, book_idx in enumerate(indices):
            if book_idx not in exclude_indices:
                # Converte la distanza angolare di Annoy in similarità coseno
                similarity_score = 1 - (distances[i]**2) / 2
                candidates.append((book_idx, similarity_score))

        # Fase 3: Applica le strategie di re-ranking in sequenza
        rerank_context = rerank_context or {}
        for reranker in self.rerankers:
            candidates = reranker.rerank(candidates, self.model, rerank_context)
            
        # Fase 4: Estrai i titoli finali
        final_indices = [idx for idx, score in candidates[:top_n]]
        return [self.model.idx_to_title[i] for i in final_indices]
        
    def _get_indices_from_titles(self, titles: List[str]) -> List[int]:
        """Converte una lista di titoli nei loro indici interi corrispondenti."""
        indices = []
        for title in titles:
            idx = self.model.title_to_idx.get(title)
            if idx is not None:
                indices.append(idx)
            else:
                self.logger.warning(f"Book '{title}' not found in the model. Skipping.")
        return indices