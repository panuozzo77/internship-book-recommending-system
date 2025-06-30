import os
import logging # Import logging
from dotenv import load_dotenv

# Gestione import in base alla struttura del progetto
# Assumiamo che questo script sia nella root del progetto
# e che `etl`, `utils`, `core`, `data_processing` siano sottodirectory.

# Per permettere import da sottodirectory se lo script è nella root
# import sys
# script_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = script_dir # o os.path.dirname(script_dir) se main_augment_script è in una subdir.
# sys.path.insert(0, project_root)


from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager # Questo ora è corretto
from core.PathRegistry import PathRegistry
import subprocess

from data_processing.book_augmenter import BookAugmenter
from data_processing.providers.google_books_provider import GoogleBooksProvider
from data_processing.providers.open_library_provider import OpenLibraryProvider
from data_processing.providers.cli_book_provider import (
    CliBookProvider,
    parse_calibre_opf_output,
    parse_goodreads_rust_scraper_output # Importa il nuovo parser
)

def main():
    # 1. SETUP DEL LOGGER PRINCIPALE
    logger_manager = LoggerManager()
    log_file_path = "logs/book_augmentation.log" # Esempio di percorso file di log
    
    # Crea la directory dei log se non esiste
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger_manager.setup_logger(
        name="BookAugmentationApp", # Nome del logger principale
        log_file=log_file_path,
        level="DEBUG", # Livello di log desiderato (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format="%(asctime)s - %(levelname)-8s - [%(name)s:%(module)s.%(funcName)s:%(lineno)d] - %(message)s"
    )
    # Da questo punto in poi, LoggerManager().get_logger() restituirà il logger configurato.
    logger: logging.Logger = logger_manager.get_logger()
    logger.info("**************** Logger Principale Configurato ****************")

    load_dotenv()
    logger.debug("Variabili d'ambiente caricate da .env (se presente).")

    try:
        config_file_path = os.getenv("CONFIG_FILE_PATH", "config.json") # Default a config.json se non in .env
        # Espandi ~ se presente nel path, per compatibilità cross-platform
        expanded_config_path = os.path.expanduser(config_file_path)

        if not os.path.exists(expanded_config_path):
             logger.warning(f"File di configurazione MongoDB non trovato in '{expanded_config_path}'. Verranno usate le impostazioni predefinite di MongoDBConnection.")
        PathRegistry().set_path("config_file", expanded_config_path)
        logger.debug(f"PathRegistry configurato con config_file: {expanded_config_path}")
    except Exception as e:
        logger.warning(f"Impossibile impostare config_file in PathRegistry: {e}. Continuo con i valori predefiniti se possibile.")

    try:
        db_connection = MongoDBConnection() # Userà il config da PathRegistry
        db = db_connection.get_database()
        logger.info(f"Connesso al database: {db.name} su host {db.client.HOST}:{db.client.PORT}")
    except Exception as e:
        logger.critical(f"Impossibile connettersi a MongoDB: {e}", exc_info=True)
        return

    providers = []
    
    try:
        open_lib_provider = OpenLibraryProvider()
        providers.append(open_lib_provider)
        logger.debug("OpenLibraryProvider aggiunto.")
    except Exception as e:
        logger.warning(f"Errore durante l'inizializzazione di OpenLibraryProvider: {e}. Provider non aggiunto.", exc_info=True)

    google_provider = GoogleBooksProvider()
    if google_provider.api_key:
        providers.append(google_provider)
        logger.debug("GoogleBooksProvider aggiunto.")
    else:
        logger.warning("GoogleBooksProvider non aggiunto: GOOGLE_BOOKS_API_KEY mancante.")

    calibre_executable = "fetch-ebook-metadata"
    try:
        system_python_executable = "/usr/bin/python3"
        if not os.path.exists(system_python_executable):
            system_python_executable = "/usr/bin/python" # Prova l'alternativa
        if not os.path.exists(system_python_executable):
            logger.warning("Nessun interprete Python di sistema trovato in /usr/bin/python3 o /usr/bin/python. Calibre potrebbe non funzionare.")
            raise FileNotFoundError("Interprete Python di sistema non trovato.")
        
        calibre_script_path = "/usr/bin/fetch-ebook-metadata" # Path completo allo script Calibre
        if not os.path.exists(calibre_script_path):
            logger.warning(f"Script '{calibre_script_path}' non trovato. CalibreCLIProvider non aggiunto.")
            raise FileNotFoundError("Script Calibre non trovato.")


        # Verifica se calibre è eseguibile (opzionale, ma buon check)
        subprocess.run([calibre_executable, '--version'], capture_output=True, check=False, timeout=5)
        calibre_cli_provider = CliBookProvider(
            provider_name="CalibreCLI",
            base_command_args=[system_python_executable, calibre_script_path],
            output_parser=parse_calibre_opf_output,
            title_option='-t',
            author_option='-a',
            pass_authors_individually=False,
            timeout=45
        )
        providers.append(calibre_cli_provider)
        logger.debug("CalibreCLIProvider aggiunto.")
    except FileNotFoundError:
        logger.warning(f"Comando '{calibre_executable}' non trovato. CalibreCLIProvider non aggiunto.")
    except Exception as e:
        logger.warning(f"Errore durante l'inizializzazione di CalibreCLIProvider: {e}. Provider non aggiunto.")


    # Configurazione per lo scraper Rust
    rust_scraper_project_dir = os.getenv("RUST_SCRAPER_DIR", "~/Documents/cloning/goodreads-metadata-scraper/src")
    expanded_rust_dir = os.path.expanduser(rust_scraper_project_dir)

    if os.path.isdir(expanded_rust_dir):
        # Verifica se 'cargo' è disponibile
        try:
            subprocess.run(['cargo', '--version'], capture_output=True, check=True, timeout=5)
            goodreads_rust_provider = CliBookProvider(
                provider_name="GoodreadsRustScraper",
                base_command_args=['cargo', 'run', '--quiet', '--manifest-path', os.path.join(expanded_rust_dir, '../Cargo.toml'), '--'],
                output_parser=parse_goodreads_rust_scraper_output,
                cwd=expanded_rust_dir, # La CWD è importante per cargo run se il Manifest non è specificato con path assoluto
                                       # o se lo script Rust si aspetta di essere in quella dir
                timeout=75 # Aumentato per possibile compilazione
            )
            providers.append(goodreads_rust_provider)
            logger.info(f"GoodreadsRustScraper provider configurato per la directory: {expanded_rust_dir}")
        except FileNotFoundError:
            logger.warning("'cargo' non trovato nel PATH. GoodreadsRustScraper non aggiunto.")
        except subprocess.CalledProcessError:
            logger.warning("'cargo --version' ha fallito. GoodreadsRustScraper non aggiunto.")
        except Exception as e:
             logger.warning(f"Errore durante l'inizializzazione di GoodreadsRustScraper: {e}. Provider non aggiunto.")
    else:
        logger.warning(f"Directory del progetto Rust Scraper non trovata: {expanded_rust_dir}. Provider non aggiunto.")


    if not providers:
        logger.error("Nessun provider di dati valido configurato. Uscita.")
        return
    
    logger.info(f"Provider configurati: {[p.get_name() for p in providers]}")

    augmenter = BookAugmenter(db, providers)
    
    input_csv_file_path = os.getenv("INPUT_CSV_PATH", "output/books_to_process.csv")
    expanded_input_csv_path = os.path.expanduser(input_csv_file_path)
    
    # Crea la directory di output se non esiste
    output_dir_for_csv = os.path.dirname(expanded_input_csv_path)
    if output_dir_for_csv and not os.path.exists(output_dir_for_csv):
        try:
            os.makedirs(output_dir_for_csv)
            logger.info(f"Creata directory per CSV di input (se necessario): {output_dir_for_csv}")
        except OSError as e:
            logger.error(f"Impossibile creare directory {output_dir_for_csv}: {e}")
            # Potresti voler uscire se la directory è critica per il CSV
            
    if not os.path.exists(expanded_input_csv_path):
        logger.error(f"File CSV di input '{expanded_input_csv_path}' non trovato! Creane uno prima di eseguire l'augmentation.")
        # Potresti voler creare un CSV vuoto o uscire.
        # Per ora, si assume che il file CSV sia generato da uno script precedente.
        # Potresti voler eseguire qui lo script di esportazione se il file non esiste.
        # from your_export_script_module import export_books_for_augmentation (Esempio)
        # export_books_for_augmentation(db, expanded_input_csv_path, limit=500)
        # logger.info(f"CSV di input generato in: {expanded_input_csv_path}")
        # if not os.path.exists(expanded_input_csv_path): # Ricontrolla dopo tentativo di generazione
        #     logger.critical("Fallimento nella generazione del CSV di input. Uscita.")
        #     return

    augmentation_limit_str = os.getenv("AUGMENTATION_LIMIT")
    augmentation_limit = int(augmentation_limit_str) if augmentation_limit_str and augmentation_limit_str.isdigit() else None
    
    logger.info(f"Avvio arricchimento con limite: {'Nessuno' if augmentation_limit is None else augmentation_limit}")
    augmenter.run_augmentation_from_csv(expanded_input_csv_path, limit=augmentation_limit)
    logger.info("**************** Processo di Arricchimento Terminato ****************")

if __name__ == '__main__':
    main()