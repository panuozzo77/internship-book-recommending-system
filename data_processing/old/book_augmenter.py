# data_processing/book_augmenter.py

import os
import shlex
import time
import requests
import subprocess
import json
from pymongo.database import Database
from dotenv import load_dotenv
from typing import List, Dict, Optional, Literal

# Aggiungi un percorso relativo per permettere l'import da altre cartelle
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.PathRegistry import PathRegistry
from core.utils.logger import LoggerManager
from etl.MongoDBConnection import MongoDBConnection

# Definiamo uno stato per il log
ProcessStatus = Literal["PROCESSED_SUCCESS", "NOT_FOUND", "API_ERROR"]

class BookDataAugmenter:
    """
    Arricchisce i dati dei libri nel database usando API esterne (Google Books)
    e strumenti da riga di comando (Calibre).
    """
    GOOGLE_API_URL = "https://www.googleapis.com/books/v1/volumes"

    def __init__(self, db: Database):
        self.db = db
        self.logger = LoggerManager().get_logger()
        
        load_dotenv()
        self.api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_BOOKS_API_KEY non trovata. Assicurati che sia nel file .env")
        
        # Nuova collezione per il tracciamento
        self.log_collection = self.db.augmentation_log
        # Assicuriamoci che ci sia un indice su book_id per ricerche veloci
        self.log_collection.create_index("book_id", unique=True)

    def find_books_to_augment(self, limit: int = 100) -> List[Dict]:
        """
        Trova libri da arricchire che non sono ancora stati processati.
        """
        self.logger.info(f"Ricerca di un massimo di {limit} libri da arricchire...")
        
        # Pipeline per trovare libri con dati mancanti E che non sono nel log
        pipeline = [
            # Fase 1: Trova i libri candidati
            {'$match': {
                "$or": [
                    {"page_count": {"$in": [None, "", 0, "0"]}},
                    {"description": {"$in": [None, ""]}}
                ]
            }},
            # Fase 2: Unisci con il log di augmentation per escludere quelli già processati
            {'$lookup': {
                'from': 'augmentation_log',
                'localField': 'book_id',
                'foreignField': 'book_id',
                'as': 'log_entry'
            }},
            # Fase 3: Filtra i documenti che NON hanno una corrispondenza nel log
            {'$match': {'log_entry': {'$size': 0}}},
            # Fase 4: Limita il numero di risultati per questo batch
            {'$limit': limit},
            # Fase 5: Recupera i dettagli dell'autore
            {'$unwind': {'path': '$author_id', 'preserveNullAndEmptyArrays': True}},
            {'$lookup': {
                'from': 'authors',
                'localField': 'author_id.author_id',
                'foreignField': 'author_id',
                'as': 'author_details'
            }},
            {'$project': {
                '_id': 0, 'book_id': 1, 'book_title': 1,
                'author_name': {'$arrayElemAt': ['$author_details.name', 0]}
            }}
        ]
        
        books_to_fix = list(self.db.books.aggregate(pipeline))
        self.logger.info(f"Trovati {len(books_to_fix)} nuovi libri da processare.")
        return books_to_fix

    def _log_process_status(self, book_id: str, status: ProcessStatus, details: str = ""):
        """Registra lo stato di un'operazione nella collezione di log."""
        self.log_collection.update_one(
            {'book_id': book_id},
            {'$set': {
                'book_id': book_id,
                'status': status,
                'last_attempt': time.time(),
                'details': details
            }},
            upsert=True # Crea il documento se non esiste
        )

    def search_google_books(self, title: str, author: Optional[str]) -> Optional[Dict]:
        """Cerca un libro su Google Books e gestisce gli errori API."""
        query = f"intitle:{title}"
        if author:
            query += f"+inauthor:{author}"
        params = {'q': query, 'key': self.api_key, 'maxResults': 1}
        
        try:
            response = requests.get(self.GOOGLE_API_URL, params=params)
            # Gestione specifica per l'errore 429
            if response.status_code == 429:
                self.logger.error("Errore 429: Too Many Requests. Il limite API è stato raggiunto.")
                raise requests.exceptions.RequestException("API Rate Limit Exceeded")
            
            response.raise_for_status()
            data = response.json()
            if data.get("items"):
                return data["items"][0].get("volumeInfo")
            return None
        except requests.exceptions.RequestException as e:
            # Rilancia l'eccezione per farla gestire dal chiamante
            raise e

    def search_with_calibre(self, title: str, author: Optional[str]) -> Optional[Dict]:
        """Cerca un libro usando lo strumento `fetch-ebook-metadata` di Calibre."""
        self.logger.info("Tentativo di fallback con Calibre `fetch-ebook-metadata`...")
        safe_title = shlex.quote(title)
        command_str = f"fetch-ebook-metadata --opf -t {safe_title}"
        

        python_executable = "/usr/bin/python3" 
        calibre_script = "/usr/bin/fetch-ebook-metadata"

        command_list = [
            python_executable,
            calibre_script,
            '--opf',
            '-t', safe_title,
        ]
        if author:
            # Aggiungi l'autore alla ricerca se fornito
            safe_author = shlex.quote(author)
            command_list.extend(['-a', safe_author])

        try:
            # Esegui il comando e cattura l'output
            result = subprocess.run(
                command_str,
                #shell=True,
                capture_output=True, 
                text=True, 
                timeout=30,
                encoding='utf-8', # Forza l'encoding
                errors='replace'  # Sostituisce caratteri problematici invece di crashare
            )
            if result.returncode != 0:
                # Logghiamo SIA stdout CHE stderr per un debug completo
                error_details = result.stderr.strip()
                output_details = result.stdout.strip()
                self.logger.warning(
                    f"Calibre ha terminato con errore (codice {result.returncode}) per '{title}'.\n"
                    f"  - STDERR: {error_details}\n"
                    f"  - STDOUT: {output_details}"
                )
                return None
            # L'output OPF è XML, ma per questo scopo possiamo fare un parsing semplice o usare regex
            # Qui estraiamo i dati in modo semplice. Per un parsing robusto si userebbe una libreria XML.
            output = result.stdout
            if not output.strip():
                self.logger.warning(f"Calibre ha avuto successo ma non ha restituito metadati per '{title}'.")
                return None
            
            self.logger.info(f"Calibre ha trovato metadati per '{title}'.")

            data_to_return = {}
            # Estrazione della descrizione
            if '<dc:description>' in output:
                desc_start = output.find('<dc:description>') + len('<dc:description>')
                desc_end = output.find('</dc:description>')
                data_to_return['description'] = output[desc_start:desc_end].strip()
            
            # Estrazione del numero di pagine (se presente in OPF)
            # Nota: il numero di pagine non è uno standard OPF, ma alcuni plugin lo aggiungono.
            # Esempio di tag non standard: <meta name="calibre:pages" content="318"/>
            if 'name="calibre:pages"' in output:
                try:
                    content_start = output.find('content="', output.find('name="calibre:pages"')) + len('content="')
                    content_end = output.find('"', content_start)
                    page_str = output[content_start:content_end]
                    if page_str.isdigit():
                        data_to_return['pageCount'] = int(page_str)
                except Exception as e:
                    self.logger.error(f"Errore durante l'estrazione delle pagine: {e}")

            return data_to_return if data_to_return else None

        except FileNotFoundError:
            self.logger.error("Comando 'fetch-ebook-metadata' non trovato. Assicurati che Calibre sia installato e nel PATH di sistema.")
            return None
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            self.logger.error(f"Errore durante l'esecuzione di Calibre: {e}")
            return None

    def update_book_in_db(self, book_id: str, new_data: Dict):
        """Aggiorna un documento libro nel database."""
        update_fields = {}
        if new_data.get('pageCount'):
            update_fields['page_count'] = new_data['pageCount']
        if new_data.get('description'):
            update_fields['description'] = new_data['description']
        
        if not update_fields:
            return
        
        result = self.db.books.update_one({'book_id': book_id}, {'$set': update_fields})
        if result.modified_count > 0:
            self.logger.info(f"Libro {book_id} aggiornato con successo. Campi: {list(update_fields.keys())}")

    def run_augmentation(self, limit: int = 100):
        """Esegue l'intero processo di arricchimento."""
        self.logger.info("--- Avvio del processo di arricchimento dati ---")
        books_to_process = self.find_books_to_augment(limit)
        
        if not books_to_process:
            self.logger.info("Nessun libro da arricchire. Processo terminato.")
            return

        for i, book in enumerate(books_to_process):
            book_id = book['book_id']
            title = book['book_title']
            author = book.get('author_name')
            self.logger.info(f"Processo libro {i+1}/{len(books_to_process)}: ID {book_id} - '{title}'")
            
            enriched_data = None
            status: ProcessStatus = "NOT_FOUND"
            details = ""

            try:
                # 1. Tenta con Google Books
                enriched_data = self.search_google_books(title, author)
                
                # 2. Se Google fallisce o non trova, tenta con Calibre come fallback
                if not enriched_data or not enriched_data.get('description'):
                    calibre_data = self.search_with_calibre(title, author)
                    if calibre_data:
                        # Unisci i risultati, dando priorità ai dati di Calibre se esistono
                        if enriched_data:
                            enriched_data.update(calibre_data)
                        else:
                            enriched_data = calibre_data
                
                if enriched_data:
                    self.update_book_in_db(book_id, enriched_data)
                    status = "PROCESSED_SUCCESS"
                else:
                    details = "Nessun dato trovato da nessuna fonte."
                    self.logger.warning(f"Nessun dato trovato per '{title}'")
                
                time.sleep(1) # Pausa per rispettare le API

            except requests.exceptions.RequestException as e:
                # Se l'errore è "Too Many Requests", interrompi tutto
                if "API Rate Limit Exceeded" in str(e):
                    self.logger.critical("Limite API di Google raggiunto. Interruzione dello script. Riprova più tardi.")
                    self._log_process_status(book_id, "API_ERROR", "Google API rate limit")
                    return # Esce dalla funzione run_augmentation
                else:
                    status = "API_ERROR"
                    details = f"Errore Google API: {e}"
                    self.logger.error(details)
            
            except Exception as e:
                status = "API_ERROR"
                details = f"Errore imprevisto: {e}"
                self.logger.critical(details, exc_info=True)

            # Registra lo stato finale dell'operazione per questo libro
            self._log_process_status(book_id, status, details)
            
        self.logger.info("--- Processo di arricchimento dati completato ---")


if __name__ == '__main__':
    logger_manager = LoggerManager()
    logger_manager.get_logger()

    PathRegistry().set_path("config_file", "/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json")

    try:
        mongo_conn = MongoDBConnection()
        db = mongo_conn.get_database()
        
        augmenter = BookDataAugmenter(db)
        augmenter.run_augmentation(limit=50) 
        
    except Exception as e:
        LoggerManager().get_logger().critical(f"Errore critico nell'esecuzione dello script: {e}", exc_info=True)