# profiler/user_profiler.py

import numpy as np
import pandas as pd # Aggiunto import per la gestione dei duplicati
from typing import List, Dict, Optional, Tuple

from core.path_registry import PathRegistry
from recommender.engine import ContentBasedAnnoyRecommender 
from utils.logger import LoggerManager

class UserProfiler:
    """
    Questa classe è responsabile della creazione di un profilo utente vettoriale
    basato su una lista di valutazioni (libri, punteggi, recensioni).
    """
    def __init__(self, recommender: ContentBasedAnnoyRecommender):
        """
        Inizializza il profiler.
        (Il costruttore rimane invariato)
        """
        self.logger = LoggerManager().get_logger()
        if not isinstance(recommender, ContentBasedAnnoyRecommender):
            raise TypeError("Il profiler richiede un'istanza valida di ContentBasedAnnoyRecommender.")
        
        self.recommender = recommender
        # Accediamo al database tramite l'istanza del recommender
        self.db = self.recommender.mongo_conn.get_database()
        self.logger.info("UserProfiler inizializzato e collegato al motore di raccomandazione.")

    # --- METODO ESISTENTE PER IL FILE (invariato) ---
    def _parse_user_ratings_file(self, file_path: str) -> List[Dict[str, any]]:
        # ... (codice del metodo come prima) ...
        user_ratings = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split(';')
                    if len(parts) != 3:
                        self.logger.warning(f"Riga malformata nel file di profilo, ignorata: '{line}'")
                        continue
                    
                    title, rating_str, review_text = parts
                    
                    try:
                        rating = int(rating_str)
                        if not 1 <= rating <= 5:
                            raise ValueError("Rating fuori dal range 1-5")
                        
                        user_ratings.append({
                            'title': title.strip(), 
                            'rating': rating, 
                            'review': review_text.strip()
                        })
                    except (ValueError, TypeError) as e:
                        self.logger.warning(f"Rating non valido per il libro '{title}', ignorato: {e}")

        except FileNotFoundError:
            self.logger.error(f"File di profilo utente non trovato: {file_path}")
            return []
        
        return user_ratings


    # --- NUOVO METODO PRIVATO PER LA QUERY AL DB ---
    def _fetch_user_ratings_from_db(self, user_id: str) -> List[Dict[str, any]]:
        """
        Recupera le valutazioni di un utente dal database, unendo 'reviews' e 'books'.
        
        Args:
            user_id: L'ID dell'utente da profilare.

        Returns:
            Una lista di dizionari con i dati delle recensioni.
        """
        self.logger.info(f"Recupero delle recensioni per l'utente {user_id} dal DB...")
        
        # Pipeline di aggregazione per trovare le recensioni e unirle ai titoli dei libri
        pipeline = [
            # Fase 1: Filtra le recensioni per l'utente specifico
            {
                '$match': {
                    'user_id': user_id
                }
            },
            # Fase 2: Unisci con la collezione 'books' per ottenere il titolo
            {
                '$lookup': {
                    'from': self.recommender.collection_name, # Usa il nome della collezione dal recommender
                    'localField': 'book_id',
                    'foreignField': 'book_id',
                    'as': 'book_details'
                }
            },
            # Fase 3: De-normalizza il risultato (solitamente un solo libro per recensione)
            {
                '$unwind': {
                    'path': '$book_details',
                    'preserveNullAndEmptyArrays': False # Escludi recensioni di libri non trovati
                }
            },
            # Fase 4: Proietta solo i campi che ci interessano in un formato pulito
            {
                '$project': {
                    '_id': 0,
                    'title': '$book_details.book_title',
                    'rating': '$rating',
                    'review': '$review_text'
                }
            }
        ]
        
        try:
            cursor = self.db.reviews.aggregate(pipeline)
            user_ratings = list(cursor)
            self.logger.info(f"Trovate {len(user_ratings)} recensioni per l'utente {user_id}.")
            return user_ratings
        except Exception as e:
            self.logger.error(f"Errore durante il recupero delle recensioni dal DB per l'utente {user_id}: {e}", exc_info=True)
            return []


    def create_weighted_profile(self, user_id: Optional[str] = None) -> Optional[Tuple[np.ndarray, set]]:
        """
        Crea un profilo utente vettoriale ponderato.
        - Se viene fornito un user_id, interroga la collezione 'reviews'.
        - Se user_id è None, interroga la collezione 'my_books'.
        
        Args:
            user_id (opzionale): L'ID dell'utente da profilare. Se None, si usa 'my_books'.

        Returns:
            Una tupla (profilo_vettoriale, indici_libri_letti) o None se fallisce.
        """
        if user_id:
            self.logger.info(f"Avvio creazione profilo per l'utente del DB: {user_id}")
            user_ratings = self._fetch_ratings_from_reviews_collection(user_id)
            source_name = f"l'utente {user_id}"
        else:
            self.logger.info("Avvio creazione profilo dalla collezione 'my_books'")
            user_ratings = self._fetch_ratings_from_my_books_collection()
            source_name = "la collezione 'my_books'"

        if not user_ratings:
            self.logger.warning(f"Nessuna recensione valida trovata per {source_name}. Impossibile creare il profilo.")
            return None
        
        # Chiama il metodo centrale di creazione del profilo
        return self._create_profile_from_ratings(user_ratings)

    def _fetch_ratings_from_reviews_collection(self, user_id: str) -> List[Dict[str, any]]:
        """Recupera le valutazioni di un utente dalla collezione 'reviews'."""
        self.logger.debug(f"Recupero recensioni per l'utente {user_id}...")
        
        pipeline = [
            {'$match': {'user_id': user_id}},
            {'$lookup': {
                'from': self.recommender.collection_name,
                'localField': 'book_id',
                'foreignField': 'book_id',
                'as': 'book_details'
            }},
            {'$unwind': {'path': '$book_details', 'preserveNullAndEmptyArrays': False}},
            {'$project': {
                '_id': 0,
                'title': '$book_details.book_title',
                'rating': '$rating',
                'review': '$review_text'
            }}
        ]
        
        try:
            cursor = self.db.reviews.aggregate(pipeline)
            return list(cursor)
        except Exception as e:
            self.logger.error(f"Errore durante il recupero da 'reviews' per l'utente {user_id}: {e}", exc_info=True)
            return []

    def _fetch_ratings_from_my_books_collection(self) -> List[Dict[str, any]]:
        """Recupera le valutazioni dalla collezione 'my_books'."""
        self.logger.debug("Recupero recensioni dalla collezione 'my_books'...")
        
        # Non serve una pipeline complessa, 'my_books' ha già il titolo.
        # Dobbiamo solo proiettare i campi nel formato standard.
        try:
            cursor = self.db.my_books.find(
                {},
                {
                    '_id': 0,
                    'title': '$book_title',
                    'rating': '$rating',
                    'review': '$review_text'
                }
            )
            return list(cursor)
        except Exception as e:
            self.logger.error(f"Errore durante il recupero da 'my_books': {e}", exc_info=True)
            return []

    # --- NUOVO METODO PUBBLICO (wrapper per la logica principale) ---
    '''
    def create_weighted_profile_from_db(self, user_id: str) -> Optional[Tuple[np.ndarray, set]]:
        """
        Crea un profilo utente vettoriale ponderato partendo da un user_id del database.
        
        Args:
            user_id: L'ID dell'utente da profilare.

        Returns:
            Una tupla (profilo_vettoriale, indici_libri_letti) o None se fallisce.
        """
        self.logger.info(f"Avvio creazione profilo per l'utente del DB: {user_id}")
        user_ratings = self._fetch_user_ratings_from_db(user_id)
        if not user_ratings:
            self.logger.warning(f"Nessuna recensione trovata o errore nel recupero per l'utente {user_id}. Impossibile creare il profilo.")
            return None
            
        # Chiama il metodo principale di creazione del profilo, che è stato generalizzato
        return self._create_profile_from_ratings(user_ratings)
    '''


    # --- METODO ESISTENTE (wrapper per la logica principale) ---
    def create_weighted_profile_from_file(self, file_path: str) -> Optional[Tuple[np.ndarray, set]]:
        """
        Crea un profilo utente vettoriale ponderato partendo da un file.
        (Ora questo metodo è un semplice wrapper)
        """
        self.logger.info(f"Avvio creazione profilo da file: {file_path}")
        user_ratings = self._parse_user_ratings_file(file_path)
        if not user_ratings:
            self.logger.error("Nessuna valutazione valida trovata nel file. Impossibile creare il profilo.")
            return None
        
        # Chiama il metodo principale di creazione del profilo
        return self._create_profile_from_ratings(user_ratings)


    # --- NUOVO METODO CENTRALE PER LA CREAZIONE DEL PROFILO ---
    def _create_profile_from_ratings(self, user_ratings: List[Dict[str, any]]) -> Optional[Tuple[np.ndarray, set]]:
        """
        Metodo principale e generalizzato che crea un profilo da una lista di valutazioni.
        
        Args:
            user_ratings: Una lista di dizionari, ognuno contenente 'title', 'rating', 'review'.

        Returns:
            Una tupla (profilo_vettoriale, indici_libri_letti) o None se fallisce.
        """
        profile_accumulator = np.zeros(self.recommender.vector_size, dtype=np.float32)
        total_weight_magnitude = 0.0
        read_book_indices = set()
        
        book_indices_map = self.recommender.book_indices_map

        for item in user_ratings:
            title = item['title']
            rating = item['rating']
            
            # Controlla se il rating è valido (potrebbe essere 0 nei dati)
            if rating == 0: continue

            if title and title in book_indices_map:
                book_idx_series = book_indices_map[title]
                book_idx = book_idx_series.iloc[0] if isinstance(book_idx_series, pd.Series) else book_idx_series
                
                read_book_indices.add(book_idx)
                
                book_vector = self.recommender.index.get_item_vector(book_idx)
                
                weight = (float(rating) - 3.0) / 2.0
                
                profile_accumulator += np.array(book_vector, dtype=np.float32) * weight
                total_weight_magnitude += abs(weight)
            else:
                self.logger.warning(f"Libro '{title}' non trovato nel dataset, ignorato per il profiling.")

        if not read_book_indices:
            self.logger.error("Nessuno dei libri nel profilo è stato trovato nel nostro dataset.")
            return None

        if total_weight_magnitude == 0:
            self.logger.warning("Il profilo utente è neutro. Creo un profilo basato sulla media semplice (fallback).")
            vectors_for_fallback = [self.recommender.index.get_item_vector(i) for i in read_book_indices]
            final_profile = np.mean(vectors_for_fallback, axis=0, dtype=np.float32)
        else:
            final_profile = profile_accumulator / total_weight_magnitude
        
        self.logger.info(f"Profilo creato con successo basato su {len(read_book_indices)} libri trovati.")
        return final_profile, read_book_indices
    
if __name__ == '__main__':

    p = PathRegistry()
    p.set_path("config_file", "/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json")
    p.set_path("processed_datasets_dir", "/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/recommendation")

    print("--- Avvio Test del Profiler e Recommender ---")

    TEST_USER_ID = "8842281e1d1347389f2ab93d60773d4d" # Esempio preso dai tuoi dati

    # Se vuoi testare la logica di 'my_books', imposta TEST_USER_ID = None
    # Prima assicurati di aver aggiunto dei libri tramite l'app web!
    # TEST_USER_ID = None

    NUM_RECOMMENDATIONS = 10

    # Inizializzazione del Logger per vedere i messaggi
    # Se il tuo LoggerManager è già configurato per stampare su console, sei a posto.
    # Altrimenti, questa configurazione di base aiuta a vedere l'output.
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s')

    try:
        # 1. Inizializza il motore di raccomandazione
        # Questo potrebbe richiedere tempo se l'indice Annoy deve essere costruito
        print("\n[FASE 1] Inizializzazione del motore di raccomandazione...")
        recommender = ContentBasedAnnoyRecommender()
        print("Motore inizializzato.")

        # 2. Inizializza il UserProfiler
        print("\n[FASE 2] Inizializzazione del profiler utente...")
        profiler = UserProfiler(recommender)
        print("Profiler inizializzato.")

        # 3. Crea il profilo utente
        print(f"\n[FASE 3] Creazione del profilo per l'input: user_id='{TEST_USER_ID if TEST_USER_ID else 'my_books'}'")
        profile_data = profiler.create_weighted_profile(user_id=TEST_USER_ID)

        if profile_data:
            user_profile_vector, read_book_indices = profile_data
            print("Profilo creato con successo.")
            print(f"L'utente ha letto {len(read_book_indices)} libri trovati nel nostro dataset.")

            # 4. Ottieni le raccomandazioni
            print(f"\n[FASE 4] Richiesta di {NUM_RECOMMENDATIONS} raccomandazioni...")
            
            # Recupera i titoli dei libri letti per mostrarli
            read_titles = [recommender.book_titles_list[i] for i in read_book_indices]
            print("\nLibri letti dall'utente (usati per il profilo):")
            for i, title in enumerate(read_titles[:10]): # Mostra al massimo i primi 10
                print(f" - {title}")
            if len(read_titles) > 10:
                print(f"   ...e altri {len(read_titles) - 10} libri.")


            recommendations = recommender.get_recommendations_by_profile(
                user_profile_vector,
                read_book_indices,
                top_n=NUM_RECOMMENDATIONS
            )

            # 5. Stampa i risultati
            if recommendations:
                print(f"\n--- TOP {NUM_RECOMMENDATIONS} RACCOMANDAZIONI TROVATE ---")
                for i, title in enumerate(recommendations):
                    print(f"{i+1}. {title}")
                print("------------------------------------------")
            else:
                print("\n!!! Non è stato possibile generare raccomandazioni. !!!")
        else:
            print("\n!!! Creazione del profilo fallita. Impossibile procedere. !!!")
            print("Controlla i log per maggiori dettagli. L'utente esiste? Ha recensioni valide?")

    except Exception as e:
        logging.critical(f"Un errore imprevisto è occorso durante il test: {e}", exc_info=True)

    print("\n--- Test completato ---")