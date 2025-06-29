# recommender/facade.py
from typing import List, Any
import pandas as pd
import numpy as np

from recommender.engine import ContentBasedRecommender
from recommender.repository import UserInteractionRepository
from recommender.model import RecommenderModel

class UserRecommenderFacade:
    """
    Fornisce un'interfaccia semplice per ottenere raccomandazioni per un utente specifico.
    Orchestra il recupero dei dati dell'utente e l'uso del motore content-based.
    """
    def __init__(
        self,
        recommender: ContentBasedRecommender,
        interaction_repo: UserInteractionRepository
    ):
        self.recommender = recommender
        self.interaction_repo = interaction_repo
        self.model: RecommenderModel = self.recommender.model # Accesso diretto al modello per comodità

    def recommend_for_user(self, user_id: Any, top_n: int = 10) -> List[str]:
        """
        Genera raccomandazioni per un utente basandosi sulla sua cronologia di letture.
        
        Questo metodo implementa la logica di profilazione:
        1. Recupera i libri letti dall'utente e i loro rating.
        2. Crea un "vettore profilo" ponderato: i libri piaciuti (rating > 3)
           contribuiscono positivamente, quelli non piaciuti (rating < 3) negativamente.
        3. Calcola la media delle pagine dei libri *molto apprezzati* (rating >= 4)
           per il re-ranking.
        4. Usa il motore content-based per trovare libri simili al profilo.
        5. Esclude i libri che l'utente ha già letto.
        """
        # 1. Recupera i libri letti dall'utente (con i rating)
        user_history_df = self.interaction_repo.find_interactions_by_user(user_id)
        
        if user_history_df.empty:
            return []

        read_book_titles = user_history_df['book_title'].tolist()
        read_indices = self.recommender._get_indices_from_titles(read_book_titles)
        if not read_indices:
            return []
        
        # 2. Crea il profilo utente ponderato
        profile_accumulator = np.zeros(self.model.vector_size, dtype=np.float32)
        total_weight_magnitude = 0.0
        liked_page_counts = []
        
        for row in user_history_df.itertuples():
            book_idx = self.model.title_to_idx.get(row.book_title)
            if book_idx is None:
                continue

            book_vector = self.model.index.get_item_vector(book_idx)
            
            # Calcola il peso basato sul rating (da -1 a +1)
            weight = (float(row.rating) - 3.0) / 2.0
            
            profile_accumulator += np.array(book_vector, dtype=np.float32) * weight
            total_weight_magnitude += abs(weight)

            # Raccogli i page_count solo per i libri molto apprezzati (voto >= 4)
            if row.rating >= 4 and pd.notna(row.page_count):
                liked_page_counts.append(float(row.page_count))

        # 3. Finalizza il profilo e il contesto per il re-ranking
        if total_weight_magnitude > 0:
            profile_vector = profile_accumulator / total_weight_magnitude
        else:
            # Fallback a media semplice se tutti i voti sono neutri (3)
            read_vectors = [self.model.index.get_item_vector(i) for i in read_indices]
            profile_vector = np.mean(read_vectors, axis=0) if read_vectors else np.zeros(self.model.vector_size)

        avg_page_count = np.mean(liked_page_counts) if liked_page_counts else 0.0
        rerank_context = {'avg_page_count': avg_page_count}
        
        # 4. Ottieni raccomandazioni usando il profilo
        recommendations = self.recommender.get_recommendations_by_profile(
            profile_vector=profile_vector,
            exclude_indices=set(read_indices),
            top_n=top_n,
            rerank_context=rerank_context
        )
        
        return recommendations