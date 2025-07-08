import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# --- CONFIGURAZIONE ---
# In un'app reale, questi valori verrebbero da un file di configurazione
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mindsdb:password@localhost:27017/?authSource=gr_recommender")
DB_NAME = os.getenv("DB_NAME", "gr_recommender")

app = Flask(__name__)
app.config["SECRET_KEY"] = "una-chiave-segreta-molto-difficile-da-indovinare"

# --- CONNESSIONE AL DATABASE ---
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    books_collection = db.books # La collezione principale dei libri
    my_books_collection = db.my_books # La nuova collezione per i libri dell'utente
    app.logger.info("Connessione a MongoDB riuscita.")
except Exception as e:
    app.logger.critical(f"Impossibile connettersi a MongoDB: {e}")
    # In un'app di produzione, qui si potrebbe gestire l'errore in modo più robusto

# --- ROUTE PRINCIPALI ---

@app.route('/')
def index():
    """Pagina principale che mostra la lista dei libri dell'utente."""
    user_books = list(my_books_collection.find().sort("last_updated", -1))
    return render_template('index.html', books=user_books)

@app.route('/add', methods=['GET', 'POST'])
def add_book():
    """Pagina per cercare e aggiungere un nuovo libro con ricerca avanzata per autore."""
    search_results = []
    query = ""

    if request.method == 'POST':
        query = request.form.get('query', '').strip()
        
        if query:
            author_query = None
            book_title_query = query

            author_pattern = r'\{(.*?)\}'
            match = re.search(author_pattern, query)

            if match:
                author_query = match.group(1).strip()
                book_title_query = re.sub(author_pattern, '', query).strip()
            
            if author_query:
                # --- INIZIO PIPELINE DI AGGREGAZIONE CORRETTA ---
                pipeline = []
                
                if book_title_query:
                    pipeline.append({
                        '$match': {
                            '$text': {'$search': book_title_query}
                        }
                    })
                
                pipeline.append({
                    '$lookup': {
                        'from': 'authors',
                        # --- MODIFICA CHIAVE QUI ---
                        # Usa il percorso corretto per il campo di join
                        'localField': 'author_id.author_id', 
                        # --------------------------
                        'foreignField': 'author_id',
                        'as': 'author_details'
                    }
                })
                
                # Questa parte non ha bisogno di modifiche, ma assicuriamoci che sia corretta
                match_criteria = {
                    'author_details.name': {'$regex': author_query, '$options': 'i'}
                }
                # Assicura che la join abbia prodotto un risultato
                match_criteria['author_details'] = {'$ne': []}
                
                pipeline.append({'$match': match_criteria})
                
                if book_title_query:
                    pipeline.append({'$addFields': {'score': {'$meta': 'textScore'}}})
                    pipeline.append({'$sort': {'score': -1}})
                else:
                    pipeline.append({'$sort': {'book_title': 1}}) # Usa 'book_title' come da tuo schema
                
                pipeline.append({'$limit': 20})
                
                print(f"Eseguo pipeline di aggregazione corretta per autore '{author_query}' e titolo '{book_title_query}'")
                search_results = list(books_collection.aggregate(pipeline))
                # --- FINE PIPELINE DI AGGREGAZIONE CORRETTA ---

            elif book_title_query:
                print(f"Eseguo ricerca testuale per '{book_title_query}'")
                search_results = list(books_collection.find(
                    {'$text': {'$search': book_title_query}},
                    {'score': {'$meta': 'textScore'}}
                ).sort([('score', {'$meta': 'textScore'})]).limit(20))
    
    return render_template('add_book.html', search_results=search_results, previous_query=query)

@app.route('/save', methods=['POST'])
def save_book():
    """Salva un nuovo libro (con recensione e rating) nella lista dell'utente."""
    book_id = request.form.get('book_id')
    book_title = request.form.get('book_title')
    rating = request.form.get('rating')
    review_text = request.form.get('review_text')

    if not all([book_id, book_title, rating]):
        flash("Dati mancanti. Assicurati di compilare tutti i campi.", "danger")
        return redirect(url_for('add_book'))

    # Controlla se il libro è già stato aggiunto
    existing = my_books_collection.find_one({'book_id': book_id})
    if existing:
        flash(f"'{book_title}' è già nella tua lista!", "warning")
        return redirect(url_for('add_book'))

    my_books_collection.insert_one({
        'book_id': book_id,
        'book_title': book_title,
        'rating': int(rating),
        'review_text': review_text,
        'last_updated': datetime.utcnow()
    })
    
    flash(f"'{book_title}' aggiunto alla tua lista!", "success")
    return redirect(url_for('index'))

@app.route('/update/<book_obj_id>', methods=['POST'])
def update_book(book_obj_id):
    """Aggiorna il rating e la recensione di un libro esistente."""
    rating = request.form.get('rating')
    review_text = request.form.get('review_text')
    
    my_books_collection.update_one(
        {'_id': ObjectId(book_obj_id)},
        {'$set': {
            'rating': int(rating),
            'review_text': review_text,
            'last_updated': datetime.utcnow()
        }}
    )
    flash("Libro aggiornato con successo!", "success")
    return redirect(url_for('index'))

@app.route('/delete/<book_obj_id>', methods=['POST'])
def delete_book(book_obj_id):
    """Rimuove un libro dalla lista dell'utente."""
    my_books_collection.delete_one({'_id': ObjectId(book_obj_id)})
    flash("Libro rimosso dalla lista.", "success")
    return redirect(url_for('index'))


if __name__ == '__main__':
    # Eseguire con 'python webapp/app.py'
    # debug=True ricarica automaticamente il server quando salvi il file
    app.run(debug=True, port=5001)