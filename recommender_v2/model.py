# recommender_v2/model.py

import tensorflow as tf
import keras
from keras import layers
import tensorflow_recommenders as tfrs
from typing import Dict, List, Optional, Any

from . import config

class UserModel(keras.Model):
    """Defines the user tower for the Two-Tower model."""
    def __init__(self, unique_user_ids: List[str]):
        super().__init__()
        self.user_embedding = layers.Embedding(
            input_dim=len(unique_user_ids),
            output_dim=config.USER_EMBEDDING_DIM,
            name="user_embedding"
        )
        self.dense_layers = keras.Sequential([
            layers.Dense(128, activation="relu"),
            layers.Dense(config.PROJECTION_DIM)
        ], name="user_projection")

    def call(self, inputs: tf.Tensor, training: Optional[bool] = None, mask: Optional[tf.Tensor] = None) -> tf.Tensor:
        """Forward pass for the user tower."""
        embeddings = self.user_embedding(inputs)
        # Reshape to ensure a 2D tensor is passed to the dense layers.
        # This handles both single-item (from .map) and batched inputs.
        embeddings = tf.reshape(embeddings, [-1, config.USER_EMBEDDING_DIM])
        return self.dense_layers(embeddings, training=training)

class BookModel(keras.Model):
    """Defines the book (item) tower for the Two-Tower model."""
    def __init__(self, unique_book_ids: List[str]):
        super().__init__()
        self.book_embedding = layers.Embedding(
            input_dim=len(unique_book_ids),
            output_dim=config.BOOK_EMBEDDING_DIM,
            name="book_embedding"
        )
        self.dense_layers = keras.Sequential([
            layers.Dense(128, activation="relu"),
            layers.Dense(config.PROJECTION_DIM)
        ], name="book_projection")

    def call(self, inputs: Any, training: Optional[bool] = None, mask: Optional[tf.Tensor] = None) -> tf.Tensor:
        """Forward pass for the book tower."""
        # The `inputs` can be a dictionary (from .map) or a tensor (from direct call)
        if isinstance(inputs, dict):
            book_tensor = inputs["book_idx"]
        else:
            book_tensor = inputs
            
        embeddings = self.book_embedding(book_tensor)
        # Reshape to ensure a 2D tensor is passed to the dense layers.
        embeddings = tf.reshape(embeddings, [-1, config.BOOK_EMBEDDING_DIM])
        return self.dense_layers(embeddings, training=training)

class TwoTowerModel(tfrs.Model):
    """
    The main Two-Tower model that combines user and book towers.
    It uses `tfrs.Model` for a simplified training loop.
    """
    def __init__(self, user_model: UserModel, book_model: BookModel, all_book_features: tf.data.Dataset):
        super().__init__()
        self.user_model = user_model
        self.book_model = book_model
        
        # The `tfrs.tasks.Retrieval` task computes the loss and metrics
        self.task = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=all_book_features.map(self.book_model)
            )
        )

    def compute_loss(self, inputs: Dict[str, tf.Tensor], training: bool = False) -> tf.Tensor:
        """
        Computes the loss for a batch of data.
        Args:
            inputs: A dictionary containing 'user_idx' and 'book_idx'.
            training: A boolean indicating if the model is in training mode.
        Returns:
            The computed loss tensor.
        """
        user_embeddings = self.user_model(inputs["user_idx"], training=training)
        positive_book_embeddings = self.book_model(inputs["book_idx"], training=training)
        
        # The task will compute the loss and update the metrics.
        return self.task(
            user_embeddings,
            positive_book_embeddings,
            compute_metrics=not training # Only compute metrics during evaluation
        )