# main_orchestrator.py

import os
import logging
import csv
from dotenv import load_dotenv
import subprocess

# Import delle classi che compongono il sistema
from core.PathRegistry import PathRegistry
from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager
from data_processing.repositories import MongoBookRepository
from data_processing.aggregators import MetadataAggregator
from data_processing.services import BookCreationService, BookUpdateService

# Import dei provider concreti
from data_processing.providers.open_library_provider import OpenLibraryProvider
from data_processing.providers.google_books_provider import GoogleBooksProvider
from data_processing.providers.cli_book_provider import (
    CliBookProvider,
    parse_goodreads_rust_scraper_output
)
# Aggiungi qui gli altri tuoi provider (es. Goodreads, Calibre)
from testing.performance_tester import PerformanceTester

def load_test_cases_from_csv(file_path: str) -> list[dict[str, str]]:
    """
    Loads test cases from a CSV file.
    The CSV is expected to have 'title' and 'author' columns.
    """
    test_cases = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Ensure the keys match the expected format
                if 'title' in row and 'author' in row:
                    test_cases.append({
                        "title": row['title'],
                        "author_name": row['author']
                    })
    except FileNotFoundError:
        logging.error(f"Test case file not found: {file_path}")
    except Exception as e:
        logging.error(f"Error reading test case file {file_path}: {e}")
    return test_cases

def main():
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config3.json')

    # --- 1. SETUP E COMPOSITION ROOT ---
    # Inizializza logger, .env, connessione DB...
    logger_manager = LoggerManager()
    os.makedirs("logs", exist_ok=True)
    logger_manager.setup_logger(name="BookOrchestratorApp", log_file="logs/orchestrator.log", level="INFO")
    logger = logger_manager.get_logger()
    
    load_dotenv()
    
    try:
        db = MongoDBConnection().get_database()
        logger.info(f"Connesso al database: {db.name}")
    except Exception as e:
        logger.critical(f"Impossibile connettersi a MongoDB: {e}", exc_info=True)
        return

    # Crea le istanze delle dipendenze
    providers = [] #[OpenLibraryProvider()] # Aggiungi qui tutti i tuoi provider
    
    '''

    rust_scraper_project_dir = os.getenv("RUST_SCRAPER_DIR")
    if rust_scraper_project_dir:
        # Espandi il percorso (es. converte ~ in /home/user)
        expanded_rust_dir = os.path.expanduser(rust_scraper_project_dir)
        
        # Verifica che la directory del progetto esista
        if os.path.isdir(expanded_rust_dir):
            # Verifica che il file Cargo.toml esista per essere sicuri che sia un progetto Rust
            if os.path.exists(os.path.join(expanded_rust_dir, 'Cargo.toml')):
                try:
                    # Controlla che 'cargo' sia eseguibile
                    subprocess.run(['cargo', '--version'], capture_output=True, check=True, timeout=10)
                    
                    # Crea l'istanza del CliBookProvider
                    goodreads_provider = CliBookProvider(
                        provider_name="GoodreadsRustScraper",
                        # Comando semplice. '--' è importante per separare gli argomenti di cargo
                        # da quelli che verranno passati al tuo eseguibile.
                        base_command_args=['cargo', 'run', '--quiet', '--'],
                        
                        # Parser specifico per l'output di questo scraper
                        output_parser=parse_goodreads_rust_scraper_output,
                        
                        # **LA MODIFICA CHIAVE È QUI:**
                        # Imposta la directory di lavoro sulla root del progetto Rust.
                        # subprocess eseguirà il comando come se fossi in quella cartella.
                        cwd=expanded_rust_dir,
                        
                        # Timeout generoso per la prima compilazione
                        timeout=120
                    )
                    providers.append(goodreads_provider)
                    logger.info(f"Provider 'GoodreadsRustScraper' configurato per essere eseguito in: {expanded_rust_dir}")

                except (FileNotFoundError, subprocess.CalledProcessError):
                    logger.warning("'cargo' non trovato nel PATH o ha restituito un errore. Goodreads provider disabilitato.")
            else:
                logger.warning(f"Cargo.toml non trovato in '{expanded_rust_dir}'. Non sembra essere una directory di progetto Rust valida. Provider disabilitato.")
        else:
            logger.warning(f"La directory dello scraper Rust '{expanded_rust_dir}' non esiste. Goodreads provider disabilitato.")
    else:
        logger.warning("Variabile d'ambiente RUST_SCRAPER_DIR non impostata. Goodreads provider disabilitato.")
    '''

    google_api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    if google_api_key:
         providers.append(GoogleBooksProvider(api_key=google_api_key))
    else:
        logger.warning("GOOGLE_BOOKS_API_KEY non trovata. GoogleBooksProvider disabilitato.")

    if not providers:
        logger.critical("Nessun provider di dati configurato. Impossibile continuare.")
        return
    
    providers.append(OpenLibraryProvider())

    repository = MongoBookRepository(db)
    aggregator = MetadataAggregator(providers)

    # Inietta le dipendenze nei servizi
    use_llm_str = os.getenv("USE_LLM_MAPPER", "false").lower()
    use_llm_mapper = use_llm_str in ["true", "1", "t"]
    ollama_host = os.getenv("OLLAMA_HOST")

    creation_service = BookCreationService(
        repository,
        aggregator,
        use_llm_mapper=use_llm_mapper,
        ollama_host=ollama_host
    )
    update_service = BookUpdateService(
        repository,
        aggregator,
        use_llm_mapper=use_llm_mapper,
        ollama_host=ollama_host
    )
    
    logger.info("Sistema inizializzato. Avvio delle operazioni di test.")
    print("-" * 60)

    # --- 2. ESECUZIONE DEI TEST DI PERFORMANCE ---
    
    # Carica i casi di test dal file CSV generato dallo scraper
    test_cases = load_test_cases_from_csv("new_releases.csv")

    # Aggiungi alcuni casi di test manuali per scenari specifici
    #test_cases.extend([
    #    {"title": "Libro Inesistente XYZ", "author_name": "Autore Fantasma"}, # Caso di fallimento
    #    {"title": "The Lord of the Rings", "author_name": "J.R.R. Tolkien"}, # Probabile duplicato se già nel DB
    #])

    if not test_cases:
        logger.critical("Nessun caso di test caricato. Interruzione.")
        return

    # Pulisci il DB prima del test per risultati consistenti (ATTENZIONE: CANCELLA I DATI!)
    # SCOMMENTA SOLO SE STAI USANDO UN DB DI TEST
    # logger.warning("Pulizia delle collezioni di test...")
    repository.counters_collection.delete_many({})
    repository.books_collection.delete_many({})
    repository.authors_collection.delete_many({})
    repository.series_collection.delete_many({})
    repository.genres_collection.delete_many({})
    repository.adder_log_collection.delete_many({})
    repository.updater_log_collection.delete_many({})
    repository.scraped_genres_collection.delete_many({})
    repository.field_sources_log_collection.delete_many({})
    
    # Inizializza ed esegui il tester
    tester = PerformanceTester(creation_service, update_service)
    
    # Esegui la suite di creazione
    tester.run_creation_test_suite(test_cases)
    
    # Estrai gli ID dei libri creati con successo per la suite di aggiornamento
    created_ids = [
        res['book_id'] for res in tester.results
        if res['test_type'] == 'creation' and res['status'] == 'SUCCESS' and res['book_id']
    ]
    
    # Esegui la suite di aggiornamento
    tester.run_update_test_suite(created_ids)
    
    # Stampa il riepilogo e salva i risultati
    tester.print_summary()
    
    # Determina se l'LLM è stato usato per il nome del file
    llm_suffix = "_llm" if use_llm_mapper else "_keyword"
    tester.save_results_to_csv(f"performance_results{llm_suffix}_others.csv")

    print("-" * 60)
    logger.info("Operazioni di test completate.")

if __name__ == '__main__':
    main()