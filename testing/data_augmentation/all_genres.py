# filename: analyze_total_unique_genres.py

import pymongo
from typing import Optional, Set

# --- CONFIGURAZIONE ---
# URI e nomi dei database
MONGO_URI_1 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db"
DB_NAME_1 = "test_db"  # Database con dati GoodreadsRustScraper

MONGO_URI_2 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db_2"
DB_NAME_2 = "test_db_2"  # Database con dati degli altri provider

# Nome della collection contenente i generi grezzi
SCRAPED_GENRES_COLLECTION = "book_genres_scraped"


def connect_to_db(uri: str, db_name: str) -> Optional[pymongo.database.Database]:
    """Si connette a un database MongoDB."""
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Connessione a '{db_name}' riuscita.")
        return client[db_name]
    except pymongo.errors.ConnectionFailure as e:
        print(f"ERRORE: Impossibile connettersi a MongoDB ({db_name}). Dettagli: {e}")
        return None

def get_total_unique_genres(db: pymongo.database.Database) -> Set[str]:
    """
    Estrae l'intero vocabolario di generi unici da una collection.
    """
    print(f"Estrazione di tutti i generi unici dal database '{db.name}'...")
    
    unique_genres: Set[str] = set()
    collection = db[SCRAPED_GENRES_COLLECTION]

    # Itera su ogni documento nella collection
    for doc in collection.find({}, {"genres": 1}):
        if "genres" in doc and isinstance(doc["genres"], dict):
            # Ripristina i generi dal formato delle chiavi del dizionario
            genres = {key.replace("_", ".") for key in doc["genres"].keys()}
            # Aggiunge i generi del documento al set totale (i duplicati vengono ignorati automaticamente)
            unique_genres.update(genres)
            
    print(f"Estrazione completata per '{db.name}'.")
    return unique_genres


if __name__ == '__main__':
    db1 = connect_to_db(MONGO_URI_1, DB_NAME_1)
    db2 = connect_to_db(MONGO_URI_2, DB_NAME_2)
    
    if db1 is not None and db2 is not None:
        # Calcola i generi totali per ciascun database
        total_genres_db1 = get_total_unique_genres(db1)
        total_genres_db2 = get_total_unique_genres(db2)
        
        # --- STAMPA DEL REPORT RIASSUNTIVO ---
        print("\n" + "="*25 + " ANALISI VOCABOLARIO GENERI TOTALI " + "="*25)
        
        print(f"\nProvider Primario (GoodreadsRustScraper - DB1):")
        print(f"  -> Numero totale di generi unici trovati: {len(total_genres_db1)}")
        
        print("\nProvider Secondari (Altri - DB2):")
        print(f"  -> Numero totale di generi unici trovati: {len(total_genres_db2)}")
        
        print("-" * 65)
        
        # Calcola e mostra le differenze tra i vocabolari
        only_in_db1 = total_genres_db1 - total_genres_db2
        only_in_db2 = total_genres_db2 - total_genres_db1
        
        print(f"\nGeneri che appaiono ESCLUSIVAMENTE in DB1 (Goodreads): {len(only_in_db1)}")
        print(f"  -> Esempi: {sorted(list(only_in_db1))[:10]}...")
        
        print(f"\nGeneri che appaiono ESCLUSIVAMENTE in DB2 (Altri): {len(only_in_db2)}")
        if only_in_db2:
            print(f"  -> Esempi: {sorted(list(only_in_db2))[:10]}...")
        else:
            print("  -> Nessuno.")
            
        print("\n" + "="*72)