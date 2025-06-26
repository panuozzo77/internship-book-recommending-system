# models/book.py
from typing import Optional, Dict, List, Any
from pymongo.database import Database
from core.PathRegistry import PathRegistry
from etl.MongoDBConnection import MongoDBConnection
from utils.logger import LoggerManager


class Author:
    """
    Una semplice classe di dati per rappresentare un autore.
    """
    def __init__(self, author_id: str, name: str, ratings_count: int = 0, text_reviews_count: int = 0):
        self.author_id = author_id
        self.name = name
        self.ratings_count = ratings_count
        self.text_reviews_count = text_reviews_count

    def __repr__(self) -> str:
        return f"<Author(id='{self.author_id}', name='{self.name}')>"
    
class Book:
    """
    Rappresenta un singolo libro, aggregando informazioni da diverse collezioni.
    """
    def __init__(self, db: Database, book_id: str):
        """
        Inizializza l'oggetto Book. I dati vengono caricati su richiesta.

        Args:
            db: Un'istanza di pymongo.database.Database per eseguire le query.
            book_id: L'ID del libro da caricare.
        """
        if db is None or not isinstance(db, Database):
            raise TypeError("È richiesta un'istanza valida del database pymongo.")
        if not book_id or not isinstance(book_id, str):
            raise ValueError("È richiesto un book_id valido (stringa).")
        self.logger = LoggerManager().get_logger()
        self._db = db
        self.book_id = book_id

        # Inizializza gli attributi a None. Verranno popolati da load_data().
        self.title: Optional[str] = None
        self.description: Optional[str] = None
        self.page_count: Optional[int] = None
        self.average_rating: Optional[float] = None
        self.work_id: Optional[str] = None

        self.authors: List[Author] = []
        self.series_id: Optional[str] = None
        self.series_name: Optional[str] = None
        self.genres: List[str] = []
        self.popular_shelves: List[str] = [] # Lista di nomi di scaffali

        self._data_loaded = False # Flag per evitare caricamenti multipli

    def __repr__(self) -> str:
            """Rappresentazione testuale dell'oggetto, utile per il debug."""
            status = "Caricato" if self._data_loaded else "Non caricato"
            return f"<Book(book_id='{self.book_id}', title='{self.title}', status='{status}')>"
    '''
    def load_data(self) -> bool:
        """
        Carica i dati del libro. Versione Definitiva con pulizia dei campi di join ($trim).
        """
        if self._data_loaded:
            return True

        pipeline: List[Dict[str, Any]] = [
            # Fase 1: Trova il libro
            {'$match': {'book_id': self.book_id}},
            
            # Fase 2: Svolgi l'array degli autori
            {'$unwind': {'path': '$author_id', 'preserveNullAndEmptyArrays': True}},
            
            # --- NUOVA FASE DI PULIZIA ---
            # Creiamo un campo temporaneo con l'ID pulito da spazi
            {'$addFields': {
                'clean_author_id': {'$trim': {'input': '$author_id.author_id'}}
            }},
            
            # Fase 3: Esegui il lookup usando il campo pulito
            {'$lookup': {
                'from': 'authors',
                'let': {'lookup_id': '$clean_author_id'}, # Definiamo una variabile locale
                'pipeline': [
                    # Pipeline interna alla collezione 'authors'
                    {'$match': {
                        '$expr': {
                            # Confronta la variabile con il campo 'author_id' della collezione 'authors',
                            # anch'esso pulito con $trim per massima sicurezza.
                            '$eq': [{'$trim': {'input': '$author_id'}}, '$$lookup_id']
                        }
                    }}
                ],
                'as': 'author_info'
            }},
            
            # Il resto della pipeline per raggruppare e unire le altre collezioni
            {'$unwind': {'path': '$author_info', 'preserveNullAndEmptyArrays': True}},
            {'$group': {
                '_id': '$_id',
                'merged_doc': {'$first': '$$ROOT'},
                'author_list': {'$push': '$author_info'}
            }},
            {'$lookup': {
                'from': 'book_genres', 'localField': 'merged_doc.book_id',
                'foreignField': 'book_id', 'as': 'genre_info'
            }},
            {'$lookup': {
                'from': 'book_series', 'localField': 'merged_doc.series',
                'foreignField': 'series_id', 'as': 'series_details'
            }},
            {'$limit': 1}
        ]

        try:
            result = list(self._db.books.aggregate(pipeline))
            
            if not result:
                self.logger.warning(f"Nessun risultato trovato per book_id {self.book_id}")
                return False

            book_data_container = result[0]
            book_data = book_data_container.get('merged_doc', {})
            
            # Popola gli attributi principali
            self.title = book_data.get('book_title')
            self.description = book_data.get('description')
            self.work_id = book_data.get('work_id')
            
            # Conversione tipi numerici
            try: self.page_count = int(book_data.get('page_count')) if book_data.get('page_count') else None
            except (ValueError, TypeError): self.page_count = None
            try: self.average_rating = float(book_data.get('average_rating')) if book_data.get('average_rating') else None
            except (ValueError, TypeError): self.average_rating = None
            
            # Popola gli Autori
            self.authors = []  # Reset per evitare duplicati
            author_list_data = book_data.get('author_list', [])
            for author_doc in author_list_data:
                if author_doc and author_doc.get('author_id') and author_doc.get('name'):
                    self.authors.append(Author(author_id=author_doc['author_id'], name=author_doc['name']))
            if not self.authors:
                self.logger.warning(f"Nessun autore trovato per book_id {self.book_id}")

            # Popola le informazioni sulla Serie
            series_data = book_data_container.get('series_details', [])
            if series_data and isinstance(series_data[0], dict):
                self.series_id = series_data[0].get('series_id')
                self.series_name = series_data[0].get('name')

            # Estrai e pulisci i Generi
            genre_data_list = book_data.get('genre_info', [])
            if genre_data_list and isinstance(genre_data_list[0], dict):
                genres_dict = genre_data_list[0].get('genres', {})
                if genres_dict:
                    self.genres = list(genres_dict.keys())

            # Estrai e pulisci i Popular Shelves
            shelves_data = book_data.get('popular_shelves', [])
            if shelves_data:
                # Escludiamo scaffali generici come 'to-read'
                excluded_shelves = {'to-read', 'currently-reading', 'owned', 'books-i-own', 'owned-books'}
                self.popular_shelves = [
                    shelf['name'] for shelf in shelves_data 
                    if 'name' in shelf and shelf['name'] not in excluded_shelves
                ]

            self._data_loaded = True
            return True

        except Exception as e:
            print(f"Errore durante il caricamento dei dati per book_id {self.book_id}: {e}")
            return False
    '''
    '''
    def load_data(self) -> bool:
        """
        Carica i dati del libro. VERSIONE FINALE EFFICIENTE con pipeline di aggregazione.
        """
        if self._data_loaded:
            return True

        pipeline: List[Dict[str, Any]] = [
            # Fase 1: Trova il libro
            {'$match': {'book_id': self.book_id}},
            
            # Fase 2: Unisci con 'book_genres'
            {'$lookup': {
                'from': 'book_genres',
                'localField': 'book_id',
                'foreignField': 'book_id',
                'as': 'genre_info'
            }},
            
            # Fase 3: Unisci con 'book_series'
            {'$lookup': {
                'from': 'book_series',
                'localField': 'series',
                'foreignField': 'series_id',
                'as': 'series_details'
            }},
            
            # Fase 4: Svolgi l'array degli autori per processarli uno a uno
            {'$unwind': {'path': '$author_id', 'preserveNullAndEmptyArrays': True}},
            
            # Fase 5: Unisci con 'authors'
            {'$lookup': {
                'from': 'authors',
                'localField': 'author_id.author_id',
                'foreignField': 'author_id',
                'as': 'author_full_details'
            }},
            
            # Fase 6: Raggruppa per rimettere insieme i dati del libro e degli autori
            {'$group': {
                '_id': '$_id', # Raggruppa per l'ID del documento libro
                'book_doc': {'$first': '$$ROOT'}, # Prendi il documento libro come base
                'authors_info': {'$push': {'$arrayElemAt': ['$author_full_details', 0]}} # Raccogli i dettagli degli autori
            }},
            
            # Fase 7: Sostituisci la radice del documento con una versione pulita
            {'$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$book_doc',
                        { 'author_details': '$authors_info' }
                    ]
                }
            }},
            {'$limit': 1}
        ]

        try:
            result = list(self._db.books.aggregate(pipeline))
            
            if not result:
                self.logger.warning(f"Nessun risultato trovato per book_id {self.book_id}")
                return False

            book_data_container = result[0]
            book_data = book_data_container.get('merged_doc', {})
            
            # Popola gli attributi principali
            self.title = book_data.get('book_title')
            self.description = book_data.get('description')
            self.work_id = book_data.get('work_id')
            
            # Conversione tipi numerici
            try: self.page_count = int(book_data.get('page_count')) if book_data.get('page_count') else None
            except (ValueError, TypeError): self.page_count = None
            try: self.average_rating = float(book_data.get('average_rating')) if book_data.get('average_rating') else None
            except (ValueError, TypeError): self.average_rating = None
            
            # Popola gli Autori
            self.authors = []  # Reset per evitare duplicati
            author_list_data = book_data.get('author_list', [])
            for author_doc in author_list_data:
                if author_doc and author_doc.get('author_id') and author_doc.get('name'):
                    self.authors.append(Author(author_id=author_doc['author_id'], name=author_doc['name'], ratings_count=int(author_doc.get('ratings_count', 0)), text_reviews_count=int(author_doc.get('text_reviews_count', 0))))
            if not self.authors:
                self.logger.warning(f"Nessun autore trovato per book_id {self.book_id}")

            # Popola le informazioni sulla Serie
            series_data = book_data_container.get('series_details', [])
            if series_data and isinstance(series_data[0], dict):
                self.series_id = series_data[0].get('series_id')
                self.series_name = series_data[0].get('name')

            # Estrai e pulisci i Generi
            genre_data_list = book_data.get('genre_info', [])
            if genre_data_list and isinstance(genre_data_list[0], dict):
                genres_dict = genre_data_list[0].get('genres', {})
                if genres_dict:
                    self.genres = list(genres_dict.keys())

            # Estrai e pulisci i Popular Shelves
            shelves_data = book_data.get('popular_shelves', [])
            if shelves_data:
                # Escludiamo scaffali generici come 'to-read'
                excluded_shelves = {'to-read', 'currently-reading', 'owned', 'books-i-own', 'owned-books'}
                self.popular_shelves = [
                    shelf['name'] for shelf in shelves_data 
                    if 'name' in shelf and shelf['name'] not in excluded_shelves
                ]

            self._data_loaded = True
            return True

        except Exception as e:
            print(f"Errore durante il caricamento dei dati per book_id {self.book_id}: {e}")
            return False
    '''

    def load_data(self) -> bool:
        """
        Carica i dati del libro. VERSIONE DI DEBUG INTENSIVO con query separate.
        """
        if self._data_loaded:
            return True

        self.logger.info(f"--- INIZIO DEBUG CARICAMENTO PER BOOK_ID: {self.book_id} ---")

        try:
            # 1. Carica il documento principale del libro
            book_data = self._db.books.find_one({'book_id': self.book_id})
            if not book_data:
                self.logger.error(f"DEBUG: Libro con book_id '{self.book_id}' non trovato nella collezione 'books'.")
                return False
            
            self.logger.info(f"DEBUG: Documento libro trovato. Titolo: '{book_data.get('book_title')}'")

            # Popola gli attributi semplici
            self.title = book_data.get('book_title')
            self.description = book_data.get('description')
            self.work_id = book_data.get('work_id')
            try: self.page_count = int(book_data.get('page_count')) if book_data.get('page_count') else None
            except (ValueError, TypeError): self.page_count = None
            try: self.average_rating = float(book_data.get('average_rating')) if book_data.get('average_rating') else None
            except (ValueError, TypeError): self.average_rating = None
            
            # 2. Carica gli AUTORI
            self.authors = []
            author_refs = book_data.get('author_id', [])
            if not author_refs:
                self.logger.warning("DEBUG: Il campo 'author_id' è mancante o vuoto nel documento del libro.")
            else:
                self.logger.info(f"DEBUG: Trovati {len(author_refs)} riferimenti ad autori. Tento il lookup per ciascuno.")
                for i, author_ref in enumerate(author_refs):
                    lookup_id = author_ref.get('author_id')
                    if not lookup_id:
                        self.logger.warning(f"DEBUG: Riferimento autore #{i+1} non ha un campo 'author_id'.")
                        continue

                    # Pulizia dell'ID per sicurezza
                    clean_lookup_id = lookup_id.strip()
                    self.logger.info(f"DEBUG: Cerco l'autore con author_id: '{clean_lookup_id}'")
                    
                    # Esegui la query per l'autore
                    author_doc = self._db.authors.find_one({'author_id': clean_lookup_id})
                    
                    if author_doc:
                        self.logger.info(f"   -> TROVATO! Nome: '{author_doc.get('name')}'")
                        self.authors.append(Author(author_id=author_doc['author_id'], name=author_doc['name'], ratings_count=author_doc['ratings_count'], text_reviews_count=author_doc['text_reviews_count']))
                    else:
                        self.logger.error(f"   -> NON TROVATO! Nessun autore con ID '{clean_lookup_id}' nella collezione 'authors'.")

            # Se dopo tutto questo, la lista è vuota, lo logghiamo
            if not self.authors:
                 self.logger.warning(f"AVVISO FINALE: Nessun autore è stato caricato per book_id {self.book_id}.")


            # 3. Carica le INFORMAZIONI SULLA SERIE
            series_refs = book_data.get('series', [])
            if series_refs:
                # Assumiamo che ci sia un solo ID di serie per libro
                series_id_to_lookup = series_refs[0].strip()
                self.logger.info(f"DEBUG: Cerco la serie con series_id: '{series_id_to_lookup}'")
                series_doc = self._db.book_series.find_one({'series_id': series_id_to_lookup})
                if series_doc:
                    self.logger.info(f"   -> TROVATA! Nome: '{series_doc.get('name')}'")
                    self.series_id = series_doc.get('series_id')
                    self.series_name = series_doc.get('name')
                else:
                    self.logger.error(f"   -> NON TROVATA! Nessuna serie con ID '{series_id_to_lookup}'.")


            # 4. Carica i GENERI e gli SCAFFALI (logica invariata)
            genre_doc = self._db.book_genres.find_one({'book_id': self.book_id})
            if genre_doc and isinstance(genre_doc.get('genres'), dict):
                self.genres = list(genre_doc['genres'].keys())
            
            shelves_data = book_data.get('popular_shelves', [])
            if shelves_data:
                excluded_shelves = {'to-read', 'currently-reading', 'owned', 'books-i-own', 'owned-books'}
                self.popular_shelves = [
                    shelf['name'] for shelf in shelves_data 
                    if 'name' in shelf and shelf['name'] not in excluded_shelves
                ]

            self._data_loaded = True
            self.logger.info(f"--- FINE DEBUG CARICAMENTO PER BOOK_ID: {self.book_id} ---")
            return True

        except Exception as e:
            self.logger.error(f"Eccezione critica durante il caricamento dei dati per book_id {self.book_id}: {e}", exc_info=True)
            return False

    def is_loaded(self) -> bool:
        """Restituisce True se i dati del libro sono stati caricati."""
        return self._data_loaded
    
    def __str__(self) -> str:
        """
        Fornisce una rappresentazione stringa leggibile e ben formattata dell'oggetto Book.
        Ideale per essere stampata a schermo per l'utente.
        """
        if not self._data_loaded:
            return f"Libro (ID: {self.book_id}) - Dati non ancora caricati. Chiama .load_data()"

        # Costruisce la stringa pezzo per pezzo
        parts = []

        # Titolo e Autori
        if self.title:
            title_str = f"--- {self.title} ---"
            parts.append(title_str)
            
            # Controlla gli autori SOLO SE c'è un titolo
            if self.authors:
                for author in self.authors:
                    parts.append(f"Author: {author.name} (ID: {author.author_id} Rating: {author.ratings_count}, #Reviews: {author.text_reviews_count})")
            
            # Aggiungi la linea di separazione, la cui lunghezza dipende dal titolo
            parts.append("-" * len(title_str))
        
        # Serie
        if self.series_name:
            parts.append(f"Serie: {self.series_name}")

        # Dettagli numerici
        details = []
        if self.page_count:
            details.append(f"{self.page_count} pagine")
        if self.average_rating:
            details.append(f"Rating medio: {self.average_rating:.2f}/5.00")
        if details:
            parts.append(" | ".join(details))

        # Descrizione
        if self.description:
            # Tronca la descrizione per non inondare la console
            desc_preview = (self.description[:250] + '...') if len(self.description) > 250 else self.description
            parts.append(f"\nDescrizione:\n{desc_preview}")
        
        # Generi e Scaffali
        if self.genres:
            parts.append(f"\nGeneri: {', '.join(self.genres)}")
        
        if self.popular_shelves:
            parts.append(f"Scaffali Popolari: {', '.join(self.popular_shelves[:10])}") # Mostra i primi 10

        # Unisce tutte le parti con un a capo
        return "\n".join(parts)
    
if __name__ == '__main__':
    # --- Setup ---
    print("Avvio test della classe Book...")
    try:
        # Ottieni una connessione al DB
        PathRegistry().set_path("config_file", "/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json")
        mongo_conn = MongoDBConnection() # Assumendo che config.json sia configurato
        db = mongo_conn.get_database()
        
        # Scegli un ID di un libro dal tuo database per il test
        TEST_BOOK_ID = "33394837" # Esempio preso dalla tua immagine
        
        # --- Creazione e Caricamento ---
        print(f"\n1. Creazione dell'oggetto per book_id: {TEST_BOOK_ID}")
        book_object = Book(db=db, book_id=TEST_BOOK_ID)
        print(f"   Oggetto creato: {book_object}")
        
        print("\n2. Caricamento dei dati dal database...")
        success = book_object.load_data()
        
        if success:
            print("   Dati caricati con successo!")
            print(f"{book_object}")

    except Exception as e:
        print(f"Errore critico durante il test: {e}")