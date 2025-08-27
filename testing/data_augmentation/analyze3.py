# filename: genre_comparison_analyzer.py

import re
import numpy as np
import pymongo
from collections import defaultdict
from typing import Dict, Tuple, List, Set, Optional
import matplotlib.pyplot as plt
import seaborn as sns

# --- CONFIGURAZIONE ---
MONGO_URI_1 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db"
DB_NAME_1 = "test_db"
MONGO_URI_2 = "mongodb://mindsdb:password@localhost:27017/?authSource=test_db_2"
DB_NAME_2 = "test_db_2"

BOOKS_COLLECTION = "books"
AUTHORS_COLLECTION = "authors"
# MODIFICA: Assicurati che il nome della collection sia corretto
# Se stai analizzando i generi mappati, usa "book_genres"
# Se stai analizzando i generi grezzi, usa "book_genres_scraped"
GENRES_COLLECTION = "book_genres" 

# --- Funzioni di connessione e normalizzazione (invariate) ---
def connect_to_db(uri: str, db_name: str) -> Optional[pymongo.database.Database]:
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        print(f"Connessione a '{db_name}' riuscita.")
        return client[db_name]
    except pymongo.errors.ConnectionFailure as e:
        print(f"ERRORE: Impossibile connettersi a MongoDB ({db_name}). Dettagli: {e}")
        return None

def normalize_author(name: str) -> str:
    if not name: return ""
    normalized = name.lower().replace('.', '')
    return " ".join(normalized.split())

def normalize_title(title: str) -> str:
    if not title: return ""
    normalized = title.lower().split(':')[0]
    normalized = re.sub(r'[^\w\s]', '', normalized)
    return " ".join(normalized.split())

def build_book_identity_map(db: pymongo.database.Database) -> Dict[str, Tuple[str, str]]:
    print(f"Costruzione della mappa di identità dei libri per il database '{db.name}'...")
    book_identity_map = {}
    author_id_to_name = {
        author['author_id']: author['name']
        for author in db[AUTHORS_COLLECTION].find({}, {"author_id": 1, "name": 1})
    }
    for book in db[BOOKS_COLLECTION].find({}, {"book_id": 1, "book_title": 1, "author_id": 1}):
        book_id, title = book.get("book_id"), book.get("book_title")
        normalized_title = normalize_title(title)
        author_link = book.get("author_id", [])
        if not all([book_id, title, author_link]): continue
        author_id = author_link[0].get('author_id')
        author_name = author_id_to_name.get(author_id)
        normalized_author = normalize_author(author_name)
        if normalized_author:
            book_identity_map[book_id] = (normalized_title, normalized_author)
    print(f"Mappa creata con {len(book_identity_map)} libri per '{db.name}'.")
    return book_identity_map

def extract_genres_by_identity(db: pymongo.database.Database, identity_map: Dict[str, Tuple[str, str]]) -> Dict[Tuple[str, str], Set[str]]:
    print(f"Estrazione dei generi per il database '{db.name}'...")
    genres_by_identity = defaultdict(set)
    for doc in db[GENRES_COLLECTION].find({}, {"book_id": 1, "genres": 1}):
        book_id, identity = doc.get("book_id"), identity_map.get(doc.get("book_id"))
        if identity and "genres" in doc and isinstance(doc["genres"], dict):
            genres = {key.replace("_", ".") for key in doc["genres"].keys()}
            genres_by_identity[identity].update(genres)
    print(f"Trovati generi per {len(genres_by_identity)} libri in '{db.name}'.")
    return genres_by_identity

# --- NUOVA FUNZIONE PER IL PLOTTING ---
def plot_jaccard_distribution(jaccard_scores: List[float], output_filename: str = "jaccard_distribution.png"):
    """
    Crea e salva un istogramma della distribuzione dei punteggi di Jaccard.
    """
    if not jaccard_scores:
        print("Nessun punteggio di Jaccard da plottare.")
        return

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    sns.histplot(jaccard_scores, bins=20, kde=True, ax=ax, color="teal")
    
    mean_score = np.mean(jaccard_scores)
    median_score = np.median(jaccard_scores)
    
    ax.axvline(mean_score, color='red', linestyle='--', label=f'Media: {mean_score:.2f}')
    ax.axvline(median_score, color='purple', linestyle=':', label=f'Mediana: {median_score:.2f}')
    
    ax.set_title("Distribuzione della Similarità dei Generi (Indice di Jaccard)", fontsize=16, fontweight='bold')
    ax.set_xlabel("Indice di Jaccard (0 = Totale Disaccordo, 1 = Accordo Perfetto)", fontsize=12)
    ax.set_ylabel("Numero di Libri", fontsize=12)
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    print(f"\nGrafico della distribuzione di Jaccard salvato come '{output_filename}'.")

def compare_genre_data(genres_db1: Dict[Tuple[str, str], Set[str]], genres_db2: Dict[Tuple[str, str], Set[str]]):
    """
    Esegue la comparazione e restituisce la lista dei punteggi di Jaccard per il plotting.
    """
    # (Il corpo della funzione rimane quasi identico, ma ora restituisce i punteggi)
    common_books = set(genres_db1.keys()) & set(genres_db2.keys())
    if not common_books:
        print("Nessun libro in comune trovato.")
        return []
    
    partial_matches = []
    # ... (tutta la logica di conteggio e classificazione rimane la stessa) ...
    identical_count = partial_match_count = total_mismatch_count = db1_only_count = db2_only_count = 0
    identical_matches, total_mismatches = [], []

    for identity in common_books:
        genres1, genres2 = genres_db1.get(identity, set()), genres_db2.get(identity, set())
        if genres1 == genres2:
            identical_count += 1
            if genres1: identical_matches.append((identity, sorted(list(genres1))))
            #continue
        if genres1 and not genres2:
            db1_only_count += 1; continue
        if not genres1 and genres2:
            db2_only_count += 1; continue
        intersection, union = genres1 & genres2, genres1 | genres2
        if intersection:
            partial_match_count += 1
            jaccard_score = len(intersection) / len(union) if union else 0
            partial_matches.append({"identity": identity, "score": jaccard_score, "common": sorted(list(intersection)), "goodreads_only": sorted(list(genres1 - genres2)), "others_only": sorted(list(genres2 - genres1))})
        elif union:
            total_mismatch_count += 1
            total_mismatches.append({"identity": identity, "goodreads_genres": sorted(list(genres1)), "others_genres": sorted(list(genres2))})
    
    # --- Stampa del Report (invariato) ---
    print("\n" + "="*25 + " ANALISI COMPARATIVA DEI GENERI " + "="*25)
    print(f"\nLibri in comune analizzati: {len(common_books)}")
    print("-" * 65)
    print(f"  -> Match Identici: {identical_count} ({identical_count / len(common_books):.2%})")
    print(f"  -> Match Parziali: {partial_match_count} ({partial_match_count / len(common_books):.2%})")
    # ... (continua a stampare il resto del report come prima) ...
    
    # --- RESTITUISCE I PUNTEGGI PER IL PLOTTING ---
    return [match['score'] for match in partial_matches]

if __name__ == '__main__':
    db1 = connect_to_db(MONGO_URI_1, DB_NAME_1)
    db2 = connect_to_db(MONGO_URI_2, DB_NAME_2)
    
    if db1 is not None and db2 is not None:
        identity_map_1 = build_book_identity_map(db1)
        identity_map_2 = build_book_identity_map(db2)
        
        genres_data_1 = extract_genres_by_identity(db1, identity_map_1)
        genres_data_2 = extract_genres_by_identity(db2, identity_map_2)
        
        # Esegue la comparazione e ottiene i punteggi
        jaccard_scores = compare_genre_data(genres_data_1, genres_data_2)

        plot_jaccard_distribution(jaccard_scores)
        
 