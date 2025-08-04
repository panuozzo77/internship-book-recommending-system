# run_v2_trainer.py

import sys
import os

# Add the project root to the Python path to allow for absolute imports
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.PathRegistry import PathRegistry
from recommender_v2.trainer import Trainer
from core.utils.LoggerManager import LoggerManager

def main():
    """
    Initializes the logger and runs the Two-Tower model trainer.
    """
    LoggerManager().setup_logger("TwoTowerTrainer", "logs/two_tower_trainer.log")
    PathRegistry().set_path('config_file', '/home/cristian/Documents/projects/pyCharm/internship-book-recommending-system/config.json')
    
    trainer = Trainer()
    trainer.run_training()

if __name__ == "__main__":
    main()