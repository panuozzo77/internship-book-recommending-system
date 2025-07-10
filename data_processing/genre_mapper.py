import re
from typing import List, Dict, Set

PREDEFINED_GENRES_KEYS = [
    "children",
    "comics, graphic",
    "fantasy, paranormal",
    "fiction",
    "history, historical fiction, biography",
    "mystery, thriller, crime",
    "non-fiction",
    "poetry",
    "romance",
    "young-adult"
]

# Dizionario di mapping: chiave = categoria predefinita, valore = set di parole chiave da cercare
GENRE_KEYWORD_MAP = {
    "mystery, thriller, crime": {"mystery", "thriller", "crime", "suspense", "detective"},
    "fantasy, paranormal": {"fantasy", "paranormal", "supernatural", "vampires", "magic", "urban-fantasy"},
    "history, historical fiction, biography": {"history", "historical", "biography", "memoir", "historical-fiction", "autobiography"},
    "romance": {"romance", "contemporary-romance", "historical-romance"},
    "young-adult": {"young-adult", "ya"},
    "children": {"children", "kids", "picture-books", "juvenile"},
    "comics, graphic": {"comics", "graphic-novels", "manga", "sequential-art"},
    "non-fiction": {"non-fiction", "nonfiction", "science", "self-help", "business", "psychology", "health", "philosophy", "travel", "cooking"},
    "poetry": {"poetry", "poems"},
    # "fiction" è un caso speciale: potrebbe essere un fallback
    "fiction": {"fiction", "contemporary", "literature", "literary-fiction", "adult-fiction", "dystopian", "sci-fi", "science-fiction", "adventure"},
}

def map_scraped_genres_to_predefined(scraped_genres: List[str]) -> Dict[str, int]:
    """
    Mappa una lista di generi in formato libero alle categorie predefinite.

    Args:
        scraped_genres: Lista di generi ottenuti dai provider (es. ["Science Fiction", "Dystopian"]).

    Returns:
        Un dizionario nel formato {"categoria_predefinita": 1, ...}.
        Il valore è sempre 1 per indicare la presenza.
    """
    if not scraped_genres:
        return {}

    mapped_genres: Set[str] = set()
    normalized_scraped_genres = " ".join(g.lower() for g in scraped_genres)

    for predefined_category, keywords in GENRE_KEYWORD_MAP.items():
        for keyword in keywords:
            # Usiamo \b per matchare parole intere e evitare che "romance" matchi "necromancer"
            if re.search(r'\b' + re.escape(keyword) + r'\b', normalized_scraped_genres):
                mapped_genres.add(predefined_category)
                break # Trovato un match per questa categoria, passa alla successiva

    # Fallback a 'fiction' se nessun'altra categoria più specifica è stata trovata
    # e se la parola 'fiction' è presente.
    if not mapped_genres and "fiction" in normalized_scraped_genres:
        mapped_genres.add("fiction")

    # Il valore per il conteggio non è specificato, quindi usiamo 1 per indicare l'associazione.
    return {genre: 1 for genre in mapped_genres}