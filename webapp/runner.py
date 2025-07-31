# webapp/runner.py

import sys
import os
from typing import Dict, Any

# Aggiungi il percorso radice del progetto al PYTHONPATH
# Questo permette a 'webapp.app' di importare moduli da altre cartelle (es. 'utils')
# se necessario in futuro.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from webapp.app import create_app  # Import the create_app factory function
from core.utils.LoggerManager import LoggerManager

def run_web_ui(app_config: Dict[str, Any]):
    """
    Initializes and runs the Flask web application using the factory pattern.
    
    Args:
        app_config: The main application configuration dictionary.
    """
    logger = LoggerManager().get_logger()
    logger.info("Initializing and starting the Web User Interface (WebUI)...")

    # Create the Flask app instance using the factory
    # The factory will handle all app-specific configurations
    app = create_app(app_config)

    # Get web server configurations from the app_config, with defaults
    web_config = app_config.get("webapp", {})
    host = web_config.get("host", "127.0.0.1")
    port = web_config.get("port", 5001)
    debug = web_config.get("debug", True)

    logger.info(f"Web server will be available at http://{host}:{port}")
    if debug:
        logger.warning("Server is running in DEBUG mode. Do not use in a production environment.")

    try:
        # Run the created app instance
        # This call is blocking and will keep the program running until the server is stopped.
        app.run(host=host, port=port, debug=debug)
    except Exception as e:
        logger.critical(f"A critical error occurred while running the web server: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Web server has been shut down.")