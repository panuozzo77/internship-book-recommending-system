# --- START OF FILE app.py ---

import os
import re
import uuid
import threading
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, g
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
from datetime import datetime
from typing import Dict, Any, Optional

# --- Core Application Components ---
from core.PathRegistry import PathRegistry
from core.recommender_factory import initialize_recommender_facade
from recommender.repository import UserRepository, BookRepository
from etl.MongoDBConnection import MongoDBConnection
from recommender.facade import UserRecommenderFacade


def create_app(app_config: Dict[str, Any]):
    """
    Creates and configures a Flask application instance using the Application Factory pattern.
    """
    app = Flask(__name__)

    # --- APP CONFIGURATION ---
    webapp_config = app_config.get("webapp", {})
    app.config["SECRET_KEY"] = webapp_config.get("secret_key", "a-very-secret-key-that-should-be-changed")

    # --- DATABASE & CORE COMPONENTS INITIALIZATION ---
    user_repo: Optional[UserRepository] = None
    recommender_facade: Optional[UserRecommenderFacade] = None

    def get_db():
        """
        Opens a new database connection if there is none yet for the
        current application context.
        """
        if 'db' not in g:
            try:
                g.db_conn = MongoDBConnection()
                g.db = g.db_conn.get_database()
            except ConnectionFailure as e:
                app.logger.critical(f"FATAL: Could not connect to MongoDB. Error: {e}")
                g.db = None
        return g.db

    @app.teardown_appcontext
    def teardown_db(exception):
        """Closes the database again at the end of the request."""
        db_conn = g.pop('db_conn', None)
        #if db_conn is not None:
            #db_conn.close_connection()

    try:
        with app.app_context():
            db = get_db()
            if db is not None:
                user_repo = UserRepository(g.db_conn)
                recommender_facade = initialize_recommender_facade()
                app.logger.info(f"Successfully connected to MongoDB database: '{db.name}'")
                if recommender_facade:
                    app.logger.info("UserRecommenderFacade initialized successfully.")
                else:
                    app.logger.error("Failed to initialize UserRecommenderFacade. Recommendations will be unavailable.")
            else:
                app.logger.error("Failed to get database connection.")

    except Exception as e:
        app.logger.critical(f"A critical error occurred during app initialization: {e}", exc_info=True)


    # --- CENTRALIZED BACKGROUND TASK LOGIC ---

    def run_profile_update_task(app_context, user_id: str):
        """
        The actual task that runs in a background thread.
        It uses the recommender facade to perform the complex update logic.
        """
        with app_context:
            try:
                if not recommender_facade:
                    app.logger.error(f"Cannot update profile for '{user_id}': RecommenderFacade is not available.")
                    return

                app.logger.info(f"Background task started: Calculating and updating profile for user '{user_id}'.")
                
                # This single method handles everything:
                # 1. Fetches user interactions.
                # 2. Calculates the taste vector.
                # 3. Saves the profile to MongoDB.
                # 4. Adds/Updates the user in the live FAISS index (handling mappings).
                profile_vector = recommender_facade._get_or_create_user_profile(user_id)
                
                if profile_vector is not None:
                    # After the profile is in the index, save the index and maps to disk
                    recommender_facade.user_profile_index.save()
                    app.logger.info(f"SUCCESS: Profile for user '{user_id}' updated and FAISS index persisted.")
                else:
                    app.logger.warning(f"Could not calculate or retrieve profile for user '{user_id}'. "
                                     "This may be normal if the user has no rated interactions.")

            except Exception as e:
                app.logger.error(f"Error in background profile update for user '{user_id}': {e}", exc_info=True)

    def trigger_profile_update_in_background(user_id: str):
        """
        A helper function to safely start the background thread.
        """
        app.logger.info(f"Scheduling profile update for user '{user_id}'.")
        thread = threading.Thread(target=run_profile_update_task, args=(app.app_context(), user_id))
        thread.daemon = True  # Allows main app to exit even if threads are running
        thread.start()

    # --- ROUTES ---

    @app.route('/')
    def index():
        """Main page showing the user's book list."""
        username = session.get('username')
        if not username:
            return redirect(url_for('login'))

        db = get_db()
        if db is None:
            flash("Database connection not available. Please try again later.", "danger")
            return render_template('index.html', books=[])

        reviews = list(db.reviews.find({'user_id': username}).sort("date_updated", -1))
        
        book_repo = BookRepository(g.db_conn)
        enriched_books = []
        for review in reviews:
            book_details = book_repo.get_book_details_by_id(review['book_id'])
            if book_details:
                review['book_details'] = book_details
            enriched_books.append(review)
            
        return render_template('index.html', books=enriched_books)

    @app.route('/add', methods=['GET', 'POST'])
    def add_book():
        """Page to search for and add a new book."""
        db = get_db()
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
                            'localField': 'book_id.author_id',
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
        
        enriched_results = []
        if search_results:
            book_repo = BookRepository(g.db_conn)
            for book in search_results:
                book_details = book_repo.get_book_details_by_id(book['book_id'])
                if book_details:
                    book['book_details'] = book_details
                enriched_results.append(book)

        return render_template('add_book.html', search_results=enriched_results, previous_query=query)

    @app.route('/save', methods=['POST'])
    def save_book():
        """Saves a new book to the user's list."""
        db = get_db()
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('add_book'))
            
        reviews_collection = db.reviews
        book_id = request.form.get('book_id')
        book_title = request.form.get('book_title')
        rating_str = request.form.get('rating')
        review_text = request.form.get('review_text')
        username = session.get('username')

        if not all([book_id, book_title, rating_str]):
            flash("Missing data. Please fill out all fields.", "danger")
            return redirect(url_for('add_book'))

        if reviews_collection.find_one({'book_id': book_id, 'user_id': username}):
            flash(f"'{book_title}' is already in your list!", "warning")
            return redirect(url_for('add_book'))

        now = datetime.utcnow()
        reviews_collection.insert_one({
            'review_id': uuid.uuid4().hex,
            'book_id': book_id,
            'user_id': username,
            'rating': int(rating_str) if rating_str else 0,
            'review_text': review_text,
            'date_added': now,
            'date_updated': now,
            'read_at': None,
            'started_at': None,
            'n_votes': 0,
            'n_comments': 0
        })
        
        flash(f"'{book_title}' added to your list!", "success")

        # --- UPDATE USER PROFILE (IN BACKGROUND) ---
        if username:
            trigger_profile_update_in_background(username)

        return redirect(url_for('index'))

    @app.route('/update/<book_obj_id>', methods=['POST'])
    def update_book(book_obj_id):
        """Updates an existing book and triggers a profile update."""
        username = session.get('username')
        if not username:
            return redirect(url_for('login'))

        db = get_db()
        if not db:
            flash("Database connection not available.", "danger")
            return redirect(url_for('index'))

        # ... (update logic as before) ...
        rating_str = request.form.get('rating')
        db.reviews.update_one(
            {'_id': ObjectId(book_obj_id), 'user_id': username},  # Ensure user can only update their own books
            {'$set': {
                'rating': int(rating_str) if rating_str else 0,
                'review_text': request.form.get('review_text'),
                'date_updated': datetime.utcnow()
            }}
        )
        flash("Book updated successfully!", "success")
        
        # --- Trigger background update ---
        trigger_profile_update_in_background(username)

        return redirect(url_for('index'))

    @app.route('/delete/<book_obj_id>', methods=['POST'])
    def delete_book(book_obj_id):
        """Removes a book and triggers a profile update."""
        username = session.get('username')
        if not username:
            return redirect(url_for('login'))

        db = get_db()
        if not db:
            flash("Database connection not available.", "danger")
            return redirect(url_for('index'))
            
        db.reviews.delete_one({'_id': ObjectId(book_obj_id), 'user_id': username})
        flash("Book removed from list.", "success")
        
        # --- Trigger background update ---
        trigger_profile_update_in_background(username)

        return redirect(url_for('index'))

    # --- AUTHENTICATION & OTHER ROUTES ---

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        """Handles user registration."""
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']
            
            if not user_repo:
                flash("User repository not available.", "danger")
                return redirect(url_for('register'))

            user_id = user_repo.create_user(username, password)
            if user_id:
                flash('Registration successful. Please log in.')
                return redirect(url_for('login'))
            else:
                flash('Username already exists.')
                return render_template('register.html', flash_messages=session.pop('_flashes', []))

        return render_template('register.html', flash_messages=session.pop('_flashes', []))

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        """Handles user login."""
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']

            if not user_repo:
                flash("User repository not available.", "danger")
                return redirect(url_for('login'))

            user = user_repo.find_user_by_username(username)
            if user and user_repo.check_password(username, password):
                session['user_id'] = str(user['_id'])
                session['username'] = user['username']
                flash('You were successfully logged in.')
                return redirect(url_for('index'))
            else:
                flash('Invalid username or password.')
                return render_template('login.html', flash_messages=session.pop('_flashes', []))

        return render_template('login.html', flash_messages=session.pop('_flashes', []))

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
        username = session.get('username')
        if not username:
            return jsonify({"error": "User not logged in"}), 401

        db = get_db()
        if db is None:
            return jsonify({"error": "Database connection not available"}), 503

        if not recommender_facade:
            return jsonify({"error": "Recommendation engine is not currently available"}), 503

        # You can choose which recommendation strategy to expose here
        recommendations = recommender_facade.recommend_with_collaborative_filtering(username, top_n=10)
        if not recommendations:
            # Fallback to content-based if collaborative fails or returns nothing
            recommendations = recommender_facade.recommend_with_content_based(username, top_n=10)
            
        enriched_recommendations = []
        if recommendations:
            book_repo = BookRepository(g.db_conn)
            for rec in recommendations:
                book_details = book_repo.get_book_details_by_id(str(rec))
                if book_details:
                    enriched_recommendations.append(book_details)

        return jsonify(enriched_recommendations)

    @app.route('/api/update_user_profile', methods=['POST'])
    def api_update_user_profile():
        """
        API endpoint to manually trigger the update of the logged-in user's taste profile.
        This is useful for debugging or scheduled updates.
        """
        username = session.get('username')
        if not username:
            return jsonify({"error": "Authentication required"}), 401

        trigger_profile_update_in_background(username)
        
        return jsonify({
            "message": f"Profile update for user '{username}' has been started in the background."
        }), 202 # 202 Accepted indicates the request is accepted for processing

    return app


