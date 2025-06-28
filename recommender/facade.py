# recommender/facade.py
from typing import List, Any
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
        1. Recupera i libri letti dall'utente.
        2. Crea un "vettore profilo" mediando i vettori dei libri letti.
        3. Calcola la media delle pagine dei libri letti per il re-ranking.
        4. Usa il motore content-based per trovare libri simili al profilo.
        5. Esclude i libri che l'utente ha già letto.
        """
        # 1. Recupera i libri letti dall'utente
        user_history_df = self.interaction_repo.find_positive_interactions_by_user(user_id)
        
        if user_history_df.empty:
            # Potremmo restituire i libri più popolari come fallback, ma per ora torniamo vuoto.
            return []

        read_book_titles = user_history_df['book_title'].tolist()
        
        # 2. Converte i titoli in indici e vettori del nostro modello
        read_indices = self.recommender._get_indices_from_titles(read_book_titles)
        if not read_indices:
            return []
            
        read_vectors = [self.model.index.get_item_vector(i) for i in read_indices]
        
        # 3. Crea il vettore profilo utente
        profile_vector = np.mean(read_vectors, axis=0)
        
        # 4. (Opzionale ma potente) Prepara il contesto per il re-ranking
        avg_page_count = user_history_df['page_count'].astype(float).mean()
        rerank_context = {'avg_page_count': avg_page_count}
        
        # 5. Ottieni raccomandazioni usando il profilo
        recommendations = self.recommender.get_recommendations_by_profile(
            profile_vector=profile_vector,
            exclude_indices=set(read_indices), # Escludi i libri già letti!
            top_n=top_n,
            rerank_context=rerank_context
        )
        
        return recommendations