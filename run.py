#!venv/bin/python

import os
import sys
import logging

# Assuming your Util directory is at the same level as app_runner.py
# If Util is inside another directory, adjust the path.
# This ensures modules in Util can be found.
current_dir = os.path.dirname(os.path.abspath(__file__))
util_dir = os.path.join(current_dir, "Util") # Assuming Util is a subdirectory
if util_dir not in sys.path:
    sys.path.insert(0, current_dir) # Add project root to find Util
    # sys.path.insert(0, util_dir) # Or add Util directly if modules are at its top level

from Util.app_initializer import initialize_app # Corrected import path
from Util.PathRegistry import PathRegistry


def main():
    registry = PathRegistry()
    project_root_path = os.path.dirname(os.path.abspath(__file__))
    registry.set_path('root', project_root_path)

    # Initialize the application (loads config, sets up logging, etc.)
    initialize_app()

    print("Application main function finished.")


if __name__ == "__main__":
    # Basic logging until initialize_app takes over
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - PRE-INIT - %(message)s")
    logging.info("Starting application from run.py...")
    main()