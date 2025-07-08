# main.py or usage_example.py

from recommender.aggregator import FeatureAggregator
from recommender.repository import BookRepository, UserInteractionRepository # Importa il nuovo repo
from recommender.facade import UserRecommenderFacade # Importa la nuova facade
from recommender.model import ModelBuilder, ModelPersister
from recommender.engine import ContentBasedRecommender, PageCountReRanker, GenrePreferenceReRanker
from etl.MongoDBConnection import MongoDBConnection
from core.PathRegistry import PathRegistry
from recommender.visualizer import UserProfileVisualizer

def run_recommender_for_user(user_id):
    """Script per caricare il modello e generare raccomandazioni per un utente."""
    # 1. Inizializza le dipendenze
    path_registry = PathRegistry()
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)
    
    # 2. Carica il modello
    model = persister.load(version="1.0")
    if not model:
        print("Impossibile caricare il modello. Eseguire prima lo script di build.")
        return
        
    # 3. Inizializza i componenti del sistema di raccomandazione
    # Il motore content-based con la sua strategia di re-ranking
    content_recommender = ContentBasedRecommender(model, rerankers=[GenrePreferenceReRanker(), PageCountReRanker()])
    
    # Il repository per le interazioni utente
    interaction_repo = UserInteractionRepository(db_conn)
    
    # La facade che unisce tutto
    user_recommender = UserRecommenderFacade(
        recommender=content_recommender,
        interaction_repo=interaction_repo
    )
    
    # --- Esempio: Raccomandazioni per l'utente 123 ---
    print(f"\nGenerazione raccomandazioni per l'utente: {user_id}")
    
    user_recommendations = user_recommender.recommend_for_user(user_id, top_n=5)
    
    if user_recommendations:
        print(f"Libri raccomandati:\n{user_recommendations}")
    else:
        print("Nessuna raccomandazione generata (l'utente potrebbe non avere una cronologia).")

def build_and_save_model_2():
    """Script per costruire e salvare il modello una tantum."""
    # 1. Inizializza le dipendenze
    db_conn = MongoDBConnection()
    path_registry = PathRegistry()
    
    # 2. Aggrega e prepara tutte le feature
    repo = BookRepository(db_conn)
    aggregator = FeatureAggregator(repo)
    features_df = aggregator.aggregate_features_for_model()
    
    if features_df.empty:
        print("Nessun dato aggregato, impossibile costruire il modello.")
        return
    
    # 3. Costruisci il modello partendo dal DataFrame arricchito
    builder = ModelBuilder()
    model = builder.build(features_df)
    
    # 4. Salva il modello
    if model:
        persister = ModelPersister(path_registry)
        persister.save(model, version="1.0")
def build_and_save_model_1():
    """Script per costruire e salvare il modello una tantum."""
    # 1. Inizializza le dipendenze
    db_conn = MongoDBConnection()
    path_registry = PathRegistry() # Assumiamo sia configurata correttamente
    
    # 2. Carica i dati
    repo = BookRepository(db_conn)
    books_df = repo.fetch_all_books_for_indexing()
    
    # 3. Costruisci il modello
    builder = ModelBuilder()
    model = builder.build(books_df)
    
    # 4. Salva il modello
    if model:
        persister = ModelPersister(path_registry)
        persister.save(model, version="1.0")

def generate_user_profile_chart(user_id):
    """Genera e salva un grafico a radar delle preferenze di genere di un utente."""
    print(f"\nGenerazione grafico del profilo per l'utente: {user_id}")

    # 1. Inizializza le dipendenze (come prima)
    path_registry = PathRegistry()
    db_conn = MongoDBConnection()
    persister = ModelPersister(path_registry)
    
    # 2. Carica il modello
    model = persister.load(version="1.0")
    if not model:
        print("Impossibile caricare il modello.")
        return
    
    # 3. Inizializza la facade (necessaria per il visualizer)
    content_recommender = ContentBasedRecommender(model) # Re-rankers non servono qui
    interaction_repo = UserInteractionRepository(db_conn)
    user_recommender_facade = UserRecommenderFacade(content_recommender, interaction_repo)

    # 4. Inizializza e usa il visualizer
    visualizer = UserProfileVisualizer(user_recommender_facade)
    chart_buffer = visualizer.create_preference_radar_chart(user_id)

    if chart_buffer:
        # Salva il grafico in un file
        output_path = f"user_{user_id}_genre_profile.png"
        with open(output_path, "wb") as f:
            f.write(chart_buffer.getvalue())
        print(f"Grafico del profilo salvato con successo in: {output_path}")
    else:
        print("Impossibile generare il grafico. Controllare i log per i dettagli.")
        
if __name__ == "__main__":
    
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    PathRegistry().set_path('processed_datasets_dir', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation')
    # Eseguire per usare il raccomandatore a livello utente
    run_recommender_for_user('8842281e1d1347389f2ab93d60773d4d')

    #build_and_save_model_2()

    #generate_user_profile_chart('8842281e1d1347389f2ab93d60773d4d')