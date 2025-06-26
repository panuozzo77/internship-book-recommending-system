# !/usr/bin/env python
# run.py
import os
import sys
from core.utils.LoggerManager import LoggerManager
from core.app_initializer import initialize_app
from core.PathRegistry import PathRegistry


def main():
    """Main application entry point"""
    logger_init = LoggerManager()
    logger = logger_init.get_logger()
    logger.info("Starting application initialization...")

    try:
        # Initialize path registry
        registry = PathRegistry()
        project_root = os.path.dirname(os.path.abspath(__file__))
        registry.set_path('root', project_root)

        # Initialize the application
        initialize_app(registry)

        logger.info("Application main function completed successfully.")
    except Exception as e:
        logger.critical(f"Application failed to initialize: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()