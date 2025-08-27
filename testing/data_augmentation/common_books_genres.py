# filename: analyze_common_books_genres.py

import re
import pymongo
from collections import defaultdict
from typing import Dict, Tuple, List, Set, Optional

# --- CONFIGURAZIONE ---
# URI e nomi dei database
MONGO_URI_1 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db"
DB_NAME_1 = "test_db"  # DB con dati GoodreadsRustScraper (verità)

MONGO_URI_2 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db_2"
DB_NAME_2 = "test_db_2"  # DB con dati degli altri provider

# Nomi delle collection
BOOKS_COLLECTION = "books"
AUTHORS_COLLECTION = "authors"
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

def normalize_author(name: str) -> str:
    """Normalizza un nome d'autore per un matching robusto."""
    if not name: return ""
    normalized = name.lower().replace('.', '')
    return " ".join(normalized.split())

def normalize_title(title: str) -> str:
    """Normalizza un titolo, rimuovendo sottotitoli e punteggiatura."""
    if not title: return ""
    normalized = title.lower().split(':')[0]
    normalized = re.sub(r'[^\w\s]', '', normalized)
    return " ".join(normalized.split())

def build_book_identity_map(db: pymongo.database.Database) -> Dict[str, Tuple[str, str]]:
    """Costruisce una mappa da book_id a (titolo_normalizzato, autore_normalizzato)."""
    print(f"Costruzione mappa di identità per il database '{db.name}'...")
    book_identity_map = {}
    
    author_id_to_name = {
        author['author_id']: author['name']
        for author in db[AUTHORS_COLLECTION].find({}, {"author_id": 1, "name": 1})
    }

    for book in db[BOOKS_COLLECTION].find({}, {"book_id": 1, "book_title": 1, "author_id": 1}):
        book_id = book.get("book_id")
        title = book.get("book_title")
        normalized_title = normalize_title(title)
        
        author_link = book.get("author_id", [])
        if not all([book_id, title, author_link, 'author_id' in author_link[0]]):
            continue
        
        author_id = author_link[0]['author_id']
        author_name = author_id_to_name.get(author_id)
        normalized_author = normalize_author(author_name)
        
        if normalized_author:
            book_identity_map[book_id] = (normalized_title, normalized_author)
            
    print(f"Mappa creata con {len(book_identity_map)} libri per '{db.name}'.")
    return book_identity_map

def extract_genres_by_identity(db: pymongo.database.Database, identity_map: Dict[str, Tuple[str, str]]) -> Dict[Tuple[str, str], Set[str]]:
    """Estrae i generi e li mappa usando la tupla (titolo, autore) come chiave."""
    print(f"Estrazione generi per il database '{db.name}'...")
    genres_by_identity = defaultdict(set)
    
    for doc in db[SCRAPED_GENRES_COLLECTION].find({}, {"book_id": 1, "genres": 1}):
        identity = identity_map.get(doc.get("book_id"))
        
        if identity and "genres" in doc and isinstance(doc["genres"], dict):
            genres = {key.replace("_", ".") for key in doc["genres"].keys()}
            genres_by_identity[identity].update(genres)
            
    print(f"Trovati generi per {len(genres_by_identity)} libri in '{db.name}'.")
    return genres_by_identity

def analyze_genres_of_common_books(
    genres_db1: Dict[Tuple[str, str], Set[str]], 
    genres_db2: Dict[Tuple[str, str], Set[str]]
):
    """
    Analizza e stampa gli insiemi di generi unici SOLO per i libri in comune.
    """
    print("\n" + "="*25 + " ANALISI GENERI SUI LIBRI COMUNI " + "="*25)
    
    # Identifica le identità (titolo, autore) comuni ai due set di dati
    common_identities = set(genres_db1.keys()) & set(genres_db2.keys())
    
    if not common_identities:
        print("Nessun libro con generi associati è stato trovato in comune tra i due database.")
        return

    # Insiemi per raccogliere tutti i generi unici trovati nei libri comuni
    all_genres_db1_common = set()
    all_genres_db2_common = set()

    for identity in common_identities:
        all_genres_db1_common.update(genres_db1.get(identity, set()))
        all_genres_db2_common.update(genres_db2.get(identity, set()))
    
    # --- Stampa del Report Finale ---
    print(f"\nNumero di libri in comune con generi trovati in entrambi i database: {len(common_identities)}\n")
    
    print("-" * 65)
    print(f"Statistiche per GoodreadsRustScraper (DB1) sui {len(common_identities)} libri comuni:")
    print(f"  -> Numero di generi unici trovati: {len(all_genres_db1_common)}")
    print(f"  -> Esempi di generi: {sorted(list(all_genres_db1_common))[:10]}...") # Stampa solo i primi 10 per brevità
    print("-" * 65)
    
    print(f"\nStatistiche per Altri Provider (DB2) sui {len(common_identities)} libri comuni:")
    print(f"  -> Numero di generi unici trovati: {len(all_genres_db2_common)}")
    print(f"  -> Esempi di generi: {sorted(list(all_genres_db2_common))[:10]}...")
    print("-" * 65)

    print("\n" + "="*72)

if __name__ == '__main__':
    db1 = connect_to_db(MONGO_URI_1, DB_NAME_1)
    db2 = connect_to_db(MONGO_URI_2, DB_NAME_2)
    
    if db1 is not None and db2 is not None:
        # 1. Costruisce le mappe di identità per entrambi i DB
        identity_map_1 = build_book_identity_map(db1)
        identity_map_2 = build_book_identity_map(db2)
        
        # 2. Estrae i generi per ogni libro, mappati per identità
        genres_data_1 = extract_genres_by_identity(db1, identity_map_1)
        genres_data_2 = extract_genres_by_identity(db2, identity_map_2)
        
        # 3. Esegue l'analisi finale solo sui libri comuni
        analyze_genres_of_common_books(genres_data_1, genres_data_2)