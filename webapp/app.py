import os
import re
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from typing import Dict, Any

# --- Core Application Components ---
from core.recommender_factory import initialize_recommender_facade
from recommender.repository import UserRepository
from etl.MongoDBConnection import MongoDBConnection

def create_app(app_config: Dict[str, Any]):
    """
    Creates and configures a Flask application instance, including core components.
    This function implements the Application Factory pattern.
    """
    app = Flask(__name__)
    
    # --- APP CONFIGURATION ---
    webapp_config = app_config.get("webapp", {})
    app.config["SECRET_KEY"] = webapp_config.get("secret_key", "a-very-secret-key-that-should-be-changed")
    
    # --- DATABASE & CORE COMPONENTS INITIALIZATION ---
    try:
        # Initialize Recommender Facade
        recommender_facade = initialize_recommender_facade()
        if recommender_facade:
            app.logger.info("UserRecommenderFacade initialized successfully.")
        else:
            app.logger.error("Failed to initialize UserRecommenderFacade.")

        # Initialize MongoDB Connection for the webapp
        db_conn = MongoDBConnection()
        db = db_conn.get_database()
        app.logger.info(f"Successfully connected to MongoDB database: '{db.name}'")

    except Exception as e:
        app.logger.critical(f"A critical error occurred during app initialization: {e}", exc_info=True)
        db = None
        db_conn = None
        recommender_facade = None
        
    # --- REPOSITORIES ---
    user_repo = UserRepository(db_conn) if db_conn is not None else None

    # --- ROUTES ---
    
    @app.route('/')
    def index():
        """Main page showing the user's book list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return render_template('index.html', books=[])
            
        my_books_collection = db.my_books
        user_books = list(my_books_collection.find().sort("last_updated", -1))
        return render_template('index.html', books=user_books)

    @app.route('/add', methods=['GET', 'POST'])
    def add_book():
        """Page to search for and add a new book."""
        if db is None:
            flash("Database connection not available.", "danger")
            return render_template('add_book.html', search_results=[], previous_query="")

        search_results = []
        query = ""
        books_collection = db.books

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
                    pipeline = []
                    if book_title_query:
                        pipeline.append({'$match': {'$text': {'$search': book_title_query}}})
                    
                    pipeline.extend([
                        {'$lookup': {
                            'from': 'authors',
                            'localField': 'author_id.author_id',
                            'foreignField': 'author_id',
                            'as': 'author_details'
                        }},
                        {'$match': {
                            'author_details.name': {'$regex': author_query, '$options': 'i'},
                            'author_details': {'$ne': []}
                        }}
                    ])
                    
                    if book_title_query:
                        pipeline.extend([
                            {'$addFields': {'score': {'$meta': 'textScore'}}},
                            {'$sort': {'score': -1}}
                        ])
                    else:
                        pipeline.append({'$sort': {'book_title': 1}})
                    
                    pipeline.append({'$limit': 20})
                    search_results = list(books_collection.aggregate(pipeline))

                elif book_title_query:
                    search_results = list(books_collection.find(
                        {'$text': {'$search': book_title_query}},
                        {'score': {'$meta': 'textScore'}}
                    ).sort([('score', {'$meta': 'textScore'})]).limit(20))
        
        return render_template('add_book.html', search_results=search_results, previous_query=query)

    @app.route('/save', methods=['POST'])
    def save_book():
        """Saves a new book to the user's list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('add_book'))
            
        my_books_collection = db.my_books
        book_id = request.form.get('book_id')
        book_title = request.form.get('book_title')
        rating_str = request.form.get('rating')
        review_text = request.form.get('review_text')

        if not all([book_id, book_title, rating_str]):
            flash("Missing data. Please fill out all fields.", "danger")
            return redirect(url_for('add_book'))

        if my_books_collection.find_one({'book_id': book_id}):
            flash(f"'{book_title}' is already in your list!", "warning")
            return redirect(url_for('add_book'))

        my_books_collection.insert_one({
            'book_id': book_id,
            'book_title': book_title,
            'rating': int(rating_str) if rating_str else 0,
            'review_text': review_text,
            'last_updated': datetime.utcnow()
        })
        
        flash(f"'{book_title}' added to your list!", "success")
        return redirect(url_for('index'))

    @app.route('/update/<book_obj_id>', methods=['POST'])
    def update_book(book_obj_id):
        """Updates an existing book in the user's list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('index'))
            
        my_books_collection = db.my_books
        rating_str = request.form.get('rating')
        review_text = request.form.get('review_text')
        
        if not rating_str:
            flash("Rating is required.", "danger")
            return redirect(url_for('index'))

        my_books_collection.update_one(
            {'_id': ObjectId(book_obj_id)},
            {'$set': {
                'rating': int(rating_str) if rating_str else 0,
                'review_text': review_text,
                'last_updated': datetime.utcnow()
            }}
        )
        flash("Book updated successfully!", "success")
        return redirect(url_for('index'))

    @app.route('/delete/<book_obj_id>', methods=['POST'])
    def delete_book(book_obj_id):
        """Removes a book from the user's list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('index'))
            
        my_books_collection = db.my_books
        my_books_collection.delete_one({'_id': ObjectId(book_obj_id)})
        flash("Book removed from list.", "success")
        return redirect(url_for('index'))

    # --- AUTHENTICATION ROUTES ---

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        """Handles user registration."""
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']
            
            if not user_repo:
                flash("User repository not available.", "danger")
                return redirect(url_for('register'))

            if user_repo.create_user(username, password):
                flash('Registration successful. Please log in.')
                return redirect(url_for('login'))
            else:
                flash('Username already exists.')
                return redirect(url_for('register'))

        return '''
            <form method="post">
                Username: <input type="text" name="username"><br>
                Password: <input type="password" name="password"><br>
                <input type="submit" value="Register">
            </form>
        '''

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        """Handles user login."""
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']

            if not user_repo:
                flash("User repository not available.", "danger")
                return redirect(url_for('login'))

            if user_repo.check_password(username, password):
                user = user_repo.find_user_by_username(username)
                if user:
                    session['user_id'] = str(user['_id'])
                    session['username'] = user['username']
                    flash('You were successfully logged in.')
                    return redirect(url_for('index'))
            else:
                flash('Invalid username or password.')
                return redirect(url_for('login'))

        return '''
            <form method="post">
                Username: <input type="text" name="username"><br>
                Password: <input type="password" name="password"><br>
                <input type="submit" value="Login">
            </form>
        '''

    @app.route('/logout')
    def logout():
        """Logs the user out."""
        session.pop('user_id', None)
        session.pop('username', None)
        flash('You were logged out.')
        return redirect(url_for('index'))

    # --- API ROUTES ---

    @app.route('/api/recommendations')
    def api_recommendations():
        """API endpoint to get recommendations for the logged-in user."""
        if 'user_id' not in session:
            return jsonify({"error": "User not logged in"}), 401

        if not recommender_facade:
            return jsonify({"error": "Recommendation engine not available"}), 503

        user_id = session['user_id']
        recommendations = recommender_facade.recommend_with_content_based(user_id, top_n=10)
        
        return jsonify(recommendations)

    return app