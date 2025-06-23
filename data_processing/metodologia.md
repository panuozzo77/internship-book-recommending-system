Interfaccia BookDataProvider: Definisce un contratto comune per tutti i provider di dati.

Provider Concreti:

    GoogleBooksProvider: Utilizza l'API di Google Books.

    CliBookProvider: Un provider generico per eseguire comandi da riga di comando (come fetch-ebook-metadata di Calibre o il tuo script Rust). È configurabile con il comando specifico e un parser per l'output.

BookAugmenter: La classe principale che orchestra il processo:

    Legge il file CSV di input.

    Itera sui libri, tentando di ottenere i dati mancanti da ciascun provider configurato, in ordine.

    Aggrega i dati ottenuti.

    Aggiorna la collezione principale books con il numero di pagine e la descrizione.

    Salva i generi nella nuova collezione book_genres_scraped con la struttura {"book_id": "...", "genres": {"genre1": 1, "genre2": 1, ...}}.

    Registra dettagliatamente ogni tentativo e l'esito nella collezione augmentation_log.