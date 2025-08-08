# filename: genre_comparison_analyzer.py

import re
import numpy as np
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

def normalize_author(name: str) -> str:
    """Normalizza un nome d'autore per gestire le differenze (es. J.R.R. vs J. R. R.)."""
    if not name:
        return ""
    
    # Converte in minuscolo
    normalized = name.lower()
    
    # Rimuove i punti, che sono una causa comune di discrepanza nelle iniziali
    normalized = normalized.replace('.', '')
    
    # Rimuove spazi doppi o iniziali/finali
    normalized = " ".join(normalized.split())
    
    return normalized

def normalize_title(title: str) -> str:
    """
    Normalizza un titolo per renderlo robusto alle differenze di sottotitoli e punteggiatura.
    Esempio: "The Movement: How Women's..." -> "the movement"
    """
    if not title:
        return ""
    
    # 1. Converte in minuscolo
    normalized = title.lower()
    
    # 2. Isola il titolo principale (spesso prima dei ':')
    normalized = normalized.split(':')[0]
    
    # 3. Rimuove la punteggiatura e caratteri non alfanumerici
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # 4. Rimuove spazi extra
    normalized = " ".join(normalized.split())
    
    return normalized

def build_book_identity_map(db: pymongo.database.Database) -> Dict[str, Tuple[str, str]]:
    """
    Costruisce una mappa da book_id a (titolo_normalizzato, nome_autore_normalizzato).
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
        normalized_title = normalize_title(title)
        
        author_link = book.get("author_id", [])
        if not author_link or not isinstance(author_link, list) or 'author_id' not in author_link[0]:
            continue
        
        author_id = author_link[0]['author_id']
        author_name = author_id_to_name.get(author_id)
        
        # --- MODIFICA CHIAVE: Normalizza anche l'autore ---
        normalized_author = normalize_author(author_name)
        
        if book_id and normalized_title and normalized_author:
            # La chiave di identità ora usa ENTRAMBI i valori normalizzati
            book_identity_map[book_id] = (normalized_title, normalized_author)
            
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
    Esegue una comparazione granulare dei dati dei generi, calcolando la similarità
    e classificando le differenze. Stampa un report dettagliato.
    """
    print("\n" + "="*25 + " ANALISI COMPARATIVA DEI GENERI " + "="*25)

    common_books = set(genres_db1.keys()) & set(genres_db2.keys())
    
    if not common_books:
        print("Nessun libro in comune trovato tra i due database. Impossibile confrontare.")
        return

    # Contatori per le nuove categorie
    identical_count = 0
    partial_match_count = 0
    total_mismatch_count = 0
    db1_only_count = 0
    db2_only_count = 0
    
    # Liste per memorizzare esempi dettagliati
    partial_matches = []
    total_mismatches = []
    identical_matches = []

    all_unique_to_db1 = set()
    all_unique_to_db2 = set()

    for identity in common_books:
        genres1 = genres_db1.get(identity, set())
        genres2 = genres_db2.get(identity, set())
        
        all_unique_to_db1.update(genres1 - genres2)
        all_unique_to_db2.update(genres2 - genres1)
        
        # Casi semplici
        if genres1 == genres2:
            identical_count += 1
            if genres1:
                identical_matches.append((identity, sorted(list(genres1))))
            continue
        
        if genres1 and not genres2:
            db1_only_count += 1
            continue
            
        if not genres1 and genres2:
            db2_only_count += 1
            continue

        # Calcolo per match parziali o totali disaccordi
        intersection = genres1 & genres2
        union = genres1 | genres2
        
        if intersection:
            partial_match_count += 1
            jaccard_score = len(intersection) / len(union) if union else 0
            partial_matches.append({
                "identity": identity,
                "score": jaccard_score,
                "common": sorted(list(intersection)),
                "goodreads_only": sorted(list(genres1 - genres2)),
                "others_only": sorted(list(genres2 - genres1))
            })
        elif union: # Se non c'è intersezione ma c'è unione, è un disaccordo totale
            total_mismatch_count += 1
            total_mismatches.append({
                "identity": identity,
                "goodreads_genres": sorted(list(genres1)),
                "others_genres": sorted(list(genres2))
            })

    # --- Stampa del Report Riepilogativo ---
    print(f"\nLibri in comune analizzati: {len(common_books)}")
    print("-" * 65)
    print(f"  -> Match Identici: {identical_count} ({identical_count / len(common_books):.2%})")
    print(f"  -> Match Parziali: {partial_match_count} ({partial_match_count / len(common_books):.2%})")
    print(f"  -> Totale Disaccordo: {total_mismatch_count} ({total_mismatch_count / len(common_books):.2%})")
    print(f"  -> Dati solo da Goodreads: {db1_only_count} ({db1_only_count / len(common_books):.2%})")
    print(f"  -> Dati solo da Altri Provider: {db2_only_count} ({db2_only_count / len(common_books):.2%})")
    print("-" * 65)
    
    # Calcolo della similarità media per i match parziali
    if partial_matches:
        avg_similarity = np.mean([m['score'] for m in partial_matches])
        print(f"Similarità media (Indice di Jaccard) per i Match Parziali: {avg_similarity:.2f}")
        print("-" * 65)

    # Ordinamento dei match parziali dal meno simile al più simile
    partial_matches.sort(key=lambda x: x['score'])

    print("\n--- Esempi di Match Parziali (i 5 meno simili) ---")
    if not partial_matches:
        print("Nessun match parziale trovato.")
    else:
        for i, match in enumerate(partial_matches[:5]):
            title, author = match['identity']
            print(f"\nLibro: '{title}' di '{author}' (Similarità: {match['score']:.2f})")
            if match["common"]:
                print(f"  -> Generi Comuni: {match['common']}")
            if match["goodreads_only"]:
                print(f"  -> Solo su Goodreads: {match['goodreads_only']}")
            if match["others_only"]:
                print(f"  -> Solo su Altri Provider: {match['others_only']}")

    print("\n--- Esempi di Totale Disaccordo (max 5) ---")
    if not total_mismatches:
        print("Nessun caso di totale disaccordo trovato.")
    else:
        for i, mismatch in enumerate(total_mismatches[:5]):
            title, author = mismatch['identity']
            print(f"\nLibro: '{title}' di '{author}'")
            print(f"  -> Goodreads: {mismatch['goodreads_genres']}")
            print(f"  -> Altri Provider: {mismatch['others_genres']}")

    print("\n--- Esempi di Match Identici (max 5) ---")
    if not identical_matches:
        print("Nessun match identico con generi non vuoti trovato.")
    else:
        for i, (identity, genres) in enumerate(identical_matches[:5]):
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