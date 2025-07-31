# recommender/config.py
from typing import Final

# --- Model Configuration ---
VECTOR_SIZE: Final[int] = 500
ANNOY_METRIC: Final[str] = 'angular' # Cosine distance
ANNOY_N_TREES: Final[int] = 50

# --- Re-ranking Configuration ---
PAGE_COUNT_LOWER_BOUND_FACTOR: Final[float] = 0.8
PAGE_COUNT_UPPER_BOUND_FACTOR: Final[float] = 1.2
PAGE_COUNT_BONUS_WEIGHT: Final[float] = 0.25
GENRE_PREFERENCE_BONUS_WEIGHT: Final[float] = 0.3 # Peso per bonus/malus basato sul genere

# --- Persistence Configuration ---
MODEL_ARTIFACTS_DIR_KEY: Final[str] = 'processed_datasets_dir'
MODEL_FILENAME_TEMPLATE: Final[str] = 'recommender_model_v{version}.joblib'

# --- Collaborative Filtering Configuration ---
COLLABORATIVE_N_NEIGHBORS: Final[int] = 15 # Number of similar users to consider
COLLABORATIVE_MIN_COMMON_BOOKS: Final[int] = 3 # Min books in common to be a valid neighbor