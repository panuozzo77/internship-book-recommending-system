# recommender_v2/trainer.py

import os
import tensorflow as tf
import keras
import pickle

from . import config
from .data_loader import DataLoader
from .model import UserModel, BookModel, TwoTowerModel
from recommender.repository import UserInteractionRepository
from etl.MongoDBConnection import MongoDBConnection
from core.utils.LoggerManager import LoggerManager

class Trainer:
    """Orchestrates the training of the Two-Tower model."""

    def __init__(self):
        self.logger = LoggerManager().get_logger()
        self.db_conn = MongoDBConnection()
        self.interaction_repo = UserInteractionRepository(self.db_conn)
        self.data_loader = DataLoader(self.interaction_repo)

    def run_training(self):
        """Main method to run the full training and saving pipeline."""
        self.logger.info("--- Starting Two-Tower Model Training ---")

        # 1. Load and prepare data
        self.logger.info("Loading interaction data...")
        interactions_df = self.data_loader.get_all_interactions()
        if interactions_df.empty:
            self.logger.error("No interaction data found. Aborting training.")
            return

        self.logger.info("Preparing TensorFlow datasets...")
        train_dataset, features = self.data_loader.prepare_dataset(interactions_df)
        candidate_dataset = self.data_loader.get_candidate_books_dataset()

        # Cache and configure the training dataset for performance
        train_dataset = train_dataset.shuffle(config.SHUFFLE_BUFFER_SIZE).batch(config.BATCH_SIZE).cache()

        # 2. Build the model
        self.logger.info("Building the Two-Tower model...")
        user_ids = list(self.data_loader.user_id_map.keys())
        book_ids = list(self.data_loader.book_id_map.keys())

        user_model = UserModel(user_ids)
        book_model = BookModel(book_ids)
        
        two_tower_model = TwoTowerModel(user_model, book_model, candidate_dataset)
        two_tower_model.compile(optimizer=keras.optimizers.Adagrad(learning_rate=config.LEARNING_RATE))

        # 3. Train the model
        self.logger.info(f"Starting model training for {config.EPOCHS} epochs...")
        two_tower_model.fit(train_dataset, epochs=config.EPOCHS)

        # 4. Save artifacts
        self.logger.info("Training complete. Saving model artifacts...")
        self.save_artifacts(user_model, book_model)
        self.logger.info("--- Training pipeline finished successfully! ---")

    def save_artifacts(self, user_model: UserModel, book_model: BookModel):
        """Saves the trained models and ID mappings."""
        os.makedirs(config.MODEL_ARTIFACTS_DIR, exist_ok=True)

        # Save the user model
        user_model_path = os.path.join(config.MODEL_ARTIFACTS_DIR, "user_model")
        user_model.save(user_model_path)
        self.logger.info(f"User model saved to {user_model_path}")

        # Save the book model
        book_model_path = os.path.join(config.MODEL_ARTIFACTS_DIR, "book_model")
        book_model.save(book_model_path)
        self.logger.info(f"Book model saved to {book_model_path}")

        # Save the ID to index mappings
        with open(os.path.join(config.MODEL_ARTIFACTS_DIR, "user_id_map.pkl"), "wb") as f:
            pickle.dump(self.data_loader.user_id_map, f)
        with open(os.path.join(config.MODEL_ARTIFACTS_DIR, "book_id_map.pkl"), "wb") as f:
            pickle.dump(self.data_loader.book_id_map, f)
        self.logger.info("ID mappings saved.")


if __name__ == "__main__":
    LoggerManager().setup_logger("TwoTowerTrainer", "logs/two_tower_trainer.log")
    trainer = Trainer()
    trainer.run_training()