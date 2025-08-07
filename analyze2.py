# filename: genre_comparison_analyzer.py

import pymongo
from collections import defaultdict
from typing import Dict, Tuple, List, Set, Optional

# --- CONFIGURAZIONE ---
# URI di connessione per i due database MongoDB
MONGO_URI_1 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db"
DB_NAME_1 = "test_db"  # Database con i dati di GoodreadsRustScraper (la "verità")

MONGO_URI_2 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db_2"
DB_NAME_2 = "test_db_2"  # Database con i dati degli altri provider

# Nomi delle collection (dovrebbero essere gli stessi in entrambi i DB)
BOOKS_COLLECTION = "books"
AUTHORS_COLLECTION = "authors"
SCRAPED_GENRES_COLLECTION = "book_genres"


def connect_to_db(uri: str, db_name: str) -> Optional[pymongo.database.Database]:
    """Si connette a un database MongoDB e restituisce l'oggetto database."""
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Connessione a '{db_name}' riuscita.")
        return client[db_name]
    except pymongo.errors.ConnectionFailure as e:
        print(f"ERRORE: Impossibile connettersi a MongoDB ({db_name}). Dettagli: {e}")
        return None

def build_book_identity_map(db: pymongo.database.Database) -> Dict[str, Tuple[str, str]]:
    """
    Costruisce una mappa da book_id a (titolo, nome_autore) per un database.
    """
    print(f"Costruzione della mappa di identità dei libri per il database '{db.name}'...")
    book_identity_map = {}
    
    author_id_to_name = {
        author['author_id']: author['name']
        for author in db[AUTHORS_COLLECTION].find({}, {"author_id": 1, "name": 1})
    }

    for book in db[BOOKS_COLLECTION].find({}, {"book_id": 1, "book_title": 1, "author_id": 1}):
        book_id = book.get("book_id")
        title = book.get("book_title")
        
        author_link = book.get("author_id", [])
        if not author_link or not isinstance(author_link, list) or 'author_id' not in author_link[0]:
            continue
        
        author_id = author_link[0]['author_id']
        author_name = author_id_to_name.get(author_id)
        
        if book_id and title and author_name:
            book_identity_map[book_id] = (title, author_name)
            
    print(f"Mappa creata con {len(book_identity_map)} libri per '{db.name}'.")
    return book_identity_map

def extract_genres_by_identity(db: pymongo.database.Database, identity_map: Dict[str, Tuple[str, str]]) -> Dict[Tuple[str, str], Set[str]]:
    """
    Estrae i generi grezzi e li mappa usando la tupla (titolo, autore) come chiave.
    """
    print(f"Estrazione dei generi grezzi per il database '{db.name}'...")
    genres_by_identity = defaultdict(set)
    
    for doc in db[SCRAPED_GENRES_COLLECTION].find({}, {"book_id": 1, "genres": 1}):
        book_id = doc.get("book_id")
        identity = identity_map.get(book_id)
        
        if identity and "genres" in doc and isinstance(doc["genres"], dict):
            genres = {key.replace("_", ".") for key in doc["genres"].keys()}
            genres_by_identity[identity].update(genres)
            
    print(f"Trovati generi per {len(genres_by_identity)} libri in '{db.name}'.")
    return genres_by_identity

def compare_genre_data(genres_db1: Dict[Tuple[str, str], Set[str]], genres_db2: Dict[Tuple[str, str], Set[str]]):
    """
    Esegue la comparazione dei dati dei generi e stampa un report dettagliato.
    """
    print("\n" + "="*25 + " ANALISI COMPARATIVA DEI GENERI " + "="*25)

    common_books = set(genres_db1.keys()) & set(genres_db2.keys())
    
    if not common_books:
        print("Nessun libro in comune trovato tra i due database. Impossibile confrontare.")
        return

    differences_count = 0
    all_unique_to_db1 = set()
    all_unique_to_db2 = set()
    
    genre_differences = {}
    
    # *** NUOVA AGGIUNTA: Lista per i match identici ***
    identical_matches = []

    for identity in common_books:
        genres1 = genres_db1[identity]
        genres2 = genres_db2[identity]
        
        if genres1 != genres2:
            differences_count += 1
            unique_to_db1 = genres1 - genres2
            unique_to_db2 = genres2 - genres1
            
            all_unique_to_db1.update(unique_to_db1)
            all_unique_to_db2.update(unique_to_db2)
            
            genre_differences[identity] = {
                "goodreads_only": sorted(list(unique_to_db1)),
                "others_only": sorted(list(unique_to_db2))
            }
        else:
            # *** NUOVA AGGIUNTA: Se i generi sono uguali (e non vuoti), li salviamo ***
            if genres1: # Aggiungiamo solo se ci sono effettivamente generi da mostrare
                identical_matches.append((identity, sorted(list(genres1))))

    # --- Stampa del Report ---
    print(f"\nLibri in comune analizzati: {len(common_books)}")
    print(f"Libri con differenze di genere: {differences_count} ({differences_count / len(common_books):.2%})")
    print(f"Libri con generi identici: {len(identical_matches)}")
    print("-" * 65)
    
    print(f"\nGeneri trovati SOLO da GoodreadsRustScraper (DB1): {len(all_unique_to_db1)}")
    print(sorted(list(all_unique_to_db1)))
    print("-" * 65)

    print(f"\nGeneri trovati SOLO da Altri Provider (DB2): {len(all_unique_to_db2)}")
    print(sorted(list(all_unique_to_db2)))
    print("-" * 65)

    print("\n--- Esempi Specifici di Differenze (max 10) ---")
    if not genre_differences:
        print("Nessuna differenza di genere trovata tra i libri in comune.")
    else:
        for i, (identity, diffs) in enumerate(genre_differences.items()):
            if i >= 10:
                break
            title, author = identity
            print(f"\nLibro: '{title}' di '{author}'")
            if diffs["goodreads_only"]:
                print(f"  -> Solo su Goodreads: {diffs['goodreads_only']}")
            if diffs["others_only"]:
                print(f"  -> Solo su Altri Provider: {diffs['others_only']}")

    # *** NUOVA AGGIUNTA: Stampa degli esempi di match identici ***
    print("\n--- Esempi di Match di Genere Identici (max 5) ---")
    if not identical_matches:
        print("Nessun match identico con generi non vuoti trovato.")
    else:
        for i, (identity, genres) in enumerate(identical_matches):
            if i >= 5:
                break
            title, author = identity
            print(f"\nLibro: '{title}' di '{author}'")
            print(f"  -> Generi Comuni: {genres}")
            
    print("\n" + "="*72)

if __name__ == '__main__':
    db1 = connect_to_db(MONGO_URI_1, DB_NAME_1)
    db2 = connect_to_db(MONGO_URI_2, DB_NAME_2)
    
    if db1 is not None and db2 is not None:
        identity_map_1 = build_book_identity_map(db1)
        identity_map_2 = build_book_identity_map(db2)
        
        genres_data_1 = extract_genres_by_identity(db1, identity_map_1)
        genres_data_2 = extract_genres_by_identity(db2, identity_map_2)
        
        compare_genre_data(genres_data_1, genres_data_2)