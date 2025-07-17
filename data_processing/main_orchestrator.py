# main_orchestrator.py

import os
import logging
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

def main():
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')

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
    
    logger.info("Sistema inizializzato. Avvio delle operazioni dimostrative.")
    print("-" * 60)

    # --- 2. DIMOSTRAZIONE DI UTILIZZO ---

    # A. Aggiungere un nuovo libro
    print(">>> CASO 1: Aggiungere un libro non esistente...")
    #result_add = creation_service.add_new_book(title="la casa dei silenzi", author_name="Donato Carrisi")
    book_id_to_update = 'add_book_5'
    result_update = update_service.update_book(identifier={"book_id": book_id_to_update})
    print(f"Esito: {result_update}\n")
    
    '''
    # B. Tentare di aggiungere un duplicato
    print(">>> CASO 2: Tentare di aggiungere un duplicato...")
    result_dup = creation_service.add_new_book(title="The Lord of the Rings", author_name="J.R.R. Tolkien")
    print(f"Esito: {result_dup}\n")

    # C. Aggiornare un libro tramite ID
    if result_add.get("status") == "SUCCESS":
        book_id_to_update = result_add["book_id"]
        print(f">>> CASO 3: Aggiornare il libro appena creato tramite ID ({book_id_to_update})...")
        result_update_id = update_service.update_book(identifier={"book_id": book_id_to_update})
        print(f"Esito: {result_update_id}\n")

    # D. Aggiornare un libro tramite titolo e autore
    print(">>> CASO 4: Aggiornare un libro tramite titolo e autore...")
    result_update_title = update_service.update_book(identifier={"title": "The Lord of the Rings", "author": "J.R.R. Tolkien"})
    print(f"Esito: {result_update_title}\n")
    
    # E. Tentare di aggiornare un libro non esistente
    print(">>> CASO 5: Tentare di aggiornare un libro non esistente...")
    result_update_fail = update_service.update_book(identifier={"book_id": "non_existent_id_123"})
    print(f"Esito: {result_update_fail}\n")

    print("-" * 60)
    logger.info("Operazioni dimostrative completate.")
    '''

if __name__ == '__main__':
    main()