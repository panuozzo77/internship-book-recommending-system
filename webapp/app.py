import os
import re
import uuid
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from typing import Dict, Any, Optional

# --- Core Application Components ---
from core.recommender_factory import initialize_recommender_facade
from recommender.repository import UserRepository
from etl.MongoDBConnection import MongoDBConnection
from recommender.taste_vector_calculator import TasteVectorCalculator
from recommender.user_profile_repository import UserProfileRepository
from recommender.model import ModelPersister
from core.PathRegistry import PathRegistry
from recommender import config as recommender_config
from recommender.user_profile_index import UserProfileIndex
from werkzeug.security import generate_password_hash

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
    db_conn = None
    db = None
    recommender_facade = None
    user_profile_index = None
    taste_vector_calculator = None
    user_repo = None
    user_profile_repo = None

    try:
        # Initialize MongoDB Connection for the webapp
        db_conn = MongoDBConnection()
        db = db_conn.get_database()
        app.logger.info(f"Successfully connected to MongoDB database: '{db.name}'")

        # --- REPOSITORIES ---
        user_repo = UserRepository(db_conn)

        # Initialize Recommender Facade
        recommender_facade = initialize_recommender_facade()
        if recommender_facade:
            app.logger.info("UserRecommenderFacade initialized successfully.")
        else:
            app.logger.error("Failed to initialize UserRecommenderFacade.")

        # Initialize User Profile Repository
        user_profile_repo = UserProfileRepository(db_conn)

        # Initialize Taste Vector Calculator
        path_registry = PathRegistry()
        persister = ModelPersister(path_registry)
        model = persister.load(version="1.0")
        if model:
            taste_vector_calculator = TasteVectorCalculator(model)
        else:
            app.logger.error("Failed to load model, TasteVectorCalculator will not work")

        # Initialize User Profile Index
        index_dir = path_registry.get_path(recommender_config.MODEL_ARTIFACTS_DIR_KEY)
        if not index_dir:
            app.logger.error(f"Could not resolve path for '{recommender_config.MODEL_ARTIFACTS_DIR_KEY}'.")
        else:
            user_index_path = os.path.join(index_dir, 'user_profile_index.faiss')
            user_profile_index = UserProfileIndex(vector_size=128, index_path=user_index_path)

    except Exception as e:
        app.logger.critical(f"A critical error occurred during app initialization: {e}", exc_info=True)
        db = None
        db_conn = None
        recommender_facade = None
        user_profile_index = None
        taste_vector_calculator = None
        user_repo = None
        user_profile_repo = None
        

    # --- ROUTES ---
    
    @app.route('/')
    def index():
        """Main page showing the user's book list."""
        username = session.get('user_id')
        if not username:
            return redirect(url_for('login'))

        if db is None:
            flash("Database connection not available.", "danger")
            return render_template('index.html', books=[])

        reviews_collection = db.reviews
        user_books = list(reviews_collection.find({'user_id': username}).sort("last_updated", -1))
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
        
        return render_template('add_book.html', search_results=search_results, previous_query=query)

    @app.route('/save', methods=['POST'])
    def save_book():
        """Saves a new book to the user's list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('add_book'))
            
        reviews_collection = db.reviews
        book_id = request.form.get('book_id')
        book_title = request.form.get('book_title')
        rating_str = request.form.get('rating')
        review_text = request.form.get('review_text')
        username = session.get('user_id')

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
        if username and taste_vector_calculator and user_profile_repo and user_profile_index and db_conn:
            def update_profile_and_index_task(app_context, username, db_conn, taste_vector_calculator, user_profile_repo, user_profile_index):
                with app_context:
                    try:
                        from recommender.repository import UserInteractionRepository
                        interaction_repo = UserInteractionRepository(db_conn)
                        user_history_df = interaction_repo.find_interactions_by_user(username)

                        if not user_history_df.empty:
                            profile_vector = taste_vector_calculator.calculate(user_history_df)
                            if profile_vector is not None:
                                user_profile_repo.save_or_update(username, profile_vector)
                                app.logger.info(f"Successfully updated profile for user '{username}'.")

                                all_profiles = user_profile_repo.get_all_profiles_except(user_id_to_exclude=None)
                                if all_profiles:
                                    profiles_for_indexing = [
                                        {'user_id': i, 'taste_vector': profile['taste_vector']}
                                        for i, profile in enumerate(all_profiles)
                                    ]
                                    if user_profile_index:
                                        user_profile_index.build(profiles_for_indexing)
                                        user_profile_index.save()
                                        app.logger.info(f"Successfully rebuilt user profile index for user {username}")
                                else:
                                    app.logger.warning("No user profiles found to rebuild index.")
                            else:
                                app.logger.warning(f"Could not calculate profile for user '{username}'.")
                        else:
                            app.logger.warning(f"No interaction history for user '{username}'.")
                    except Exception as e:
                        app.logger.error(f"Error in background profile update: {e}", exc_info=True)

            import threading
            thread = threading.Thread(target=update_profile_and_index_task, args=(app.app_context(), username, db_conn, taste_vector_calculator, user_profile_repo, user_profile_index))
            thread.start()

        return redirect(url_for('index'))

    @app.route('/update/<book_obj_id>', methods=['POST'])
    def update_book(book_obj_id):
        """Updates an existing book in the user's list."""
        if db is None:
            flash("Database connection not available.", "danger")
            return redirect(url_for('index'))
            
        reviews_collection = db.reviews
        rating_str = request.form.get('rating')
        review_text = request.form.get('review_text')
        
        if not rating_str:
            flash("Rating is required.", "danger")
            return redirect(url_for('index'))

        reviews_collection.update_one(
            {'_id': ObjectId(book_obj_id)},
            {'$set': {
                'rating': int(rating_str) if rating_str else 0,
                'review_text': review_text,
                'date_updated': datetime.utcnow()
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
            
        reviews_collection = db.reviews
        reviews_collection.delete_one({'_id': ObjectId(book_obj_id)})
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

            user_id = user_repo.create_user(username, password)
            if user_id:
                flash('Registration successful. Please log in.')
                return redirect(url_for('login'))
            else:
                flash('Username already exists.')
                return render_template('register.html', flash_messages=dict(session.pop('_flashes', []) or []))

        return render_template('register.html', flash_messages=dict(session.pop('_flashes', []) or []))

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
                return render_template('login.html', flash_messages=dict(session.pop('_flashes', []) or []))

        return render_template('login.html', flash_messages=dict(session.pop('_flashes', []) or []))

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
        username = session.get('user_id')
        if not username:
            return jsonify({"error": "User not logged in"}), 401

        if not recommender_facade:
            return jsonify({"error": "Recommendation engine not available"}), 503

        recommendations = recommender_facade.recommend_with_content_based(username, top_n=10)
        
        return jsonify(recommendations)

    return app