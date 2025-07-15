import re
import requests
import json
from typing import List, Dict, Set, Optional
from core.utils.LoggerManager import LoggerManager

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

class LLMGenreMapper:
    """
    Usa un modello LLM locale (tramite Ollama) per mappare i generi.
    """
    def __init__(self, ollama_host: str, model: str = "gemma3:12b"):
        self.ollama_host = ollama_host
        self.model = model
        self.logger = LoggerManager().get_logger()

    def _build_prompt(self, title: str, authors: List[str], description: str, scraped_genres: List[str]) -> str:
        """Costruisce il prompt per l'LLM, includendo dettagli del libro."""
        genres_str = ", ".join(f'"{g}"' for g in scraped_genres)
        predefined_genres_str = ", ".join(f'"{g}"' for g in PREDEFINED_GENRES_KEYS)
        authors_str = ", ".join(authors)
        
        # Tronca la descrizione per non superare i limiti del prompt
        truncated_description = (description[:500] + '...') if len(description) > 500 else description

        return (
            "You are a librarian expert in book categorization. Your task is to analyze the provided book details and map its themes to a predefined set of categories.\n\n"
            f"**Book Title:** {title}\n"
            f"**Author(s):** {authors_str}\n"
            f"**Description:** {truncated_description}\n"
            f"**User-Generated Genres:** [{genres_str}]\n\n"
            "Based on all the information above, map the book to one or more of the following predefined categories:\n"
            f"[{predefined_genres_str}]\n\n"
            "**Rules:**\n"
            "1. Return a JSON object where keys are the matching predefined categories and values are always 1.\n"
            "2. Prioritize the book's description and title over user-generated genres if they conflict.\n"
            "3. If no specific category fits, but the genres or description suggest general fiction, use 'fiction' as a fallback.\n"
            "4. If no category fits at all, return an empty JSON object {}.\n"
            "**Example for a sci-fi novel:** {\"fiction\": 1}\n"
            "**Example for a historical romance:** {\"romance\": 1, \"history, historical fiction, biography\": 1}\n"
            "Your response must be only the JSON object, without any additional text or markdown."
        )

    def map_genres(self, title: str, authors: List[str], description: str, scraped_genres: List[str]) -> Optional[Dict[str, int]]:
        """
        Interroga l'LLM di Ollama per mappare i generi, usando i dettagli del libro.
        """
        if not scraped_genres:
            return {}

        prompt = self._build_prompt(title, authors, description, scraped_genres)
        
        response_text = ""  # Inizializza per evitare UnboundLocalError
        try:
            self.logger.info(f"Querying Ollama model '{self.model}' for genre mapping...")
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json"
                },
                timeout=60  # Timeout di 60 secondi
            )
            response.raise_for_status()
            
            response_text = response.json().get("response", "{}")
            self.logger.debug(f"Ollama raw response: {response_text}")
            
            # Pulisce la risposta per assicurarsi che sia solo JSON valido
            json_response_str = response_text.strip().replace("```json", "").replace("```", "")
            
            mapped_genres = json.loads(json_response_str)
            
            # Valida che il risultato sia nel formato atteso
            if not isinstance(mapped_genres, dict) or not all(isinstance(k, str) and v == 1 for k, v in mapped_genres.items()):
                self.logger.warning(f"Ollama response is not in the expected format: {mapped_genres}")
                return None

            self.logger.info(f"Ollama mapped genres: {list(mapped_genres.keys())}")
            return mapped_genres

        except requests.Timeout:
            self.logger.error(f"Ollama request timed out after 60 seconds.")
            return None
        except requests.RequestException as e:
            self.logger.error(f"Error querying Ollama: {e}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON from Ollama response: {e}\nResponse text: {response_text}")
            return None