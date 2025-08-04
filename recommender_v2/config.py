# recommender_v2/config.py

# --- Model Architecture ---
USER_EMBEDDING_DIM = 64
BOOK_EMBEDDING_DIM = 64
PROJECTION_DIM = 32  # Dimension after the dense projection layers

# --- Training Parameters ---
BATCH_SIZE = 4096
LEARNING_RATE = 0.001
EPOCHS = 10
SHUFFLE_BUFFER_SIZE = 100_000

# --- FAISS Indexing ---
FAISS_INDEX_FILENAME = "book_embedding_index.faiss"
FAISS_MAPPING_FILENAME = "book_id_mapping.pkl"

# --- Paths and Directories ---
MODEL_ARTIFACTS_DIR = "recommender_v2/artifacts"
MODEL_CHECKPOINT_DIR = "recommender_v2/checkpoints"