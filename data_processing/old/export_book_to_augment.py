import os
import csv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.path_registry import PathRegistry
from etl.MongoDBConnection import MongoDBConnection
from utils.logger import LoggerManager

def export_books_for_augmentation(db, output_file_path: str, limit: int = 500):
    """
    Trova libri con dati mancanti, li unisce con i loro autori (aggregati in una singola stringa),
    li ordina per numero di recensioni e li esporta in un file CSV.
    """
    logger = LoggerManager().get_logger()
    logger.info(f"Ricerca di un massimo di {limit} libri da esportare per l'arricchimento, ordinati per numero di recensioni...")

    # Pipeline per trovare i libri, aggregare gli autori, ordinare e limitare
    pipeline = [
        # 1. Filtra i libri con dati mancanti (page_count o description)
        {'$match': {
            "$or": [
                {"page_count": {"$in": [None, "", 0, "0"]}}, # 'num_pages' mappa a 'page_count'
                {"description": {"$in": [None, ""]}}
            ]
        }},
        # 2. Escludi libri già processati cercando in 'augmentation_log'
        {'$lookup': {
            'from': 'augmentation_log',
            'localField': 'book_id',
            'foreignField': 'book_id',
            'as': 'log_entry'
        }},
        {'$match': {'log_entry': {'$size': 0}}},

        # 3. Recupera i dettagli degli autori associati al libro
        # Il campo 'author_id' nella collezione 'books' è una lista di oggetti,
        # ognuno contenente un 'author_id' (es: [{ "author_id": "id1" }, { "author_id": "id2" }]).
        # 'localField': 'author_id.author_id' estrarrà gli ID ["id1", "id2"] per il lookup.
        {'$lookup': {
            'from': 'authors',
            'localField': 'author_id.author_id', # Corretto basandosi sul mapping e uso precedente
            'foreignField': 'author_id',
            'as': 'author_details_list' # Risultato: array di documenti autore
        }},

        # 4. Aggiungi un campo con i nomi degli autori concatenati
        # e prepara il campo per l'ordinamento (se necessario, ma text_reviews_count è già int)
        {'$addFields': {
            'author_names_str': {
                '$let': {
                    'vars': {
                        # Estrai i nomi dalla lista di autori e filtra quelli non validi (null o stringhe vuote)
                        'valid_names': {
                            '$filter': {
                                'input': '$author_details_list.name',
                                'as': 'name_item',
                                'cond': {'$and': [
                                    {'$ne': ['$$name_item', None]},
                                    {'$ne': ['$$name_item', '']}
                                ]}
                            }
                        }
                    },
                    # Concatena i nomi validi in una singola stringa separata da ", "
                    'in': {
                        '$reduce': {
                            'input': '$$valid_names',
                            'initialValue': '',
                            'in': {
                                '$cond': {
                                    'if': {'$eq': ['$$value', '']},
                                    'then': '$$this',
                                    'else': {'$concat': ['$$value', ', ', '$$this']}
                                }
                            }
                        }
                    }
                }
            }
            # text_reviews_count è già presente nella collezione 'books' ed è di tipo 'int'
        }},

        # 5. Ordina i libri per numero di recensioni testuali (decrescente)
        # Assumiamo che 'text_reviews_count' sia un campo numerico nella collezione 'books'.
        {'$sort': {'text_reviews_count': -1}},

        # 6. Limita il numero di risultati dopo l'ordinamento
        {'$limit': limit},

        # 7. Proietta i campi finali per il CSV
        {'$project': {
            '_id': 0,
            'book_id': 1,
            'book_title': '$book_title', # 'title' nel JSON mappa a 'book_title' in MongoDB
            'author_name': {
                # Se author_names_str è vuoto (nessun autore o nomi non validi), usa "Unknown Author"
                '$cond': {
                    'if': {'$eq': ['$author_names_str', '']},
                    'then': 'Unknown Author', # O puoi lasciare '' se preferisci
                    'else': '$author_names_str'
                }
            }
        }}
    ]

    try:
        logger.debug(f"Esecuzione pipeline di aggregazione: {pipeline}")
        books_to_export = list(db.books.aggregate(pipeline))
        
        if not books_to_export:
            logger.info("Nessun nuovo libro da esportare trovato che soddisfi i criteri.")
            return

        # Scrittura su file CSV
        # Assicurati che il percorso di output esista o crea la directory
        output_dir = os.path.dirname(output_file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Creata directory di output: {output_dir}")

        with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['book_id', 'book_title', 'author_name'])
            writer.writeheader()
            writer.writerows(books_to_export)
        
        logger.info(f"Esportati con successo {len(books_to_export)} libri in '{output_file_path}'")

    except Exception as e:
        logger.critical(f"Errore durante l'esportazione dei libri: {e}", exc_info=True)


if __name__ == '__main__':
    logger_manager = LoggerManager()
    logger_manager.get_logger() # Inizializza e ottiene il logger
    
    # Definisci il file di output
    # Potrebbe essere utile renderlo configurabile o basato su PathRegistry
    OUTPUT_CSV_PATH = "output/books_to_process.csv" # Esempio con sottodirectory
    
    # Assicurati che PathRegistry sia inizializzato correttamente se lo usi per config_file
    # Questa linea è specifica del tuo ambiente, assicurati che il path sia corretto.
    try:
        PathRegistry().set_path("config_file", "/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json")
    except Exception as e:
        LoggerManager().get_logger().warning(f"Impossibile impostare config_file in PathRegistry: {e}. Continuo con i valori predefiniti se possibile.")

    try:
        db_connection = MongoDBConnection() # Usa le impostazioni da config.json se PathRegistry è configurato
        db = db_connection.get_database()
        export_books_for_augmentation(db, OUTPUT_CSV_PATH, limit=500) # Puoi cambiare il limite qui se necessario
    except Exception as e:
        LoggerManager().get_logger().critical(f"Esecuzione dello script fallita: {e}", exc_info=True)