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

from webapp.app import app  # Importa l'istanza 'app' di Flask dal tuo file app.py
from core.utils.LoggerManager import LoggerManager

def run_web_ui(app_config: Dict[str, Any]):
    """
    Configura e avvia il server di sviluppo di Flask.
    
    Args:
        app_config: Il dizionario di configurazione principale dell'applicazione.
    """
    logger = LoggerManager().get_logger()
    logger.info("Avvio dell'interfaccia utente web (WebUI)...")

    # Recupera le configurazioni per il server web, con dei fallback di default
    web_config = app_config.get("webapp", {})
    host = web_config.get("host", "127.0.0.1")
    port = web_config.get("port", 5001)
    debug = web_config.get("debug", True)

    logger.info(f"Il server web sarà disponibile su http://{host}:{port}")
    if debug:
        logger.warning("Il server sta girando in modalità DEBUG. Non usare in produzione.")

    try:
        # L'app Flask viene eseguita qui.
        # Questa chiamata è bloccante, il che significa che il programma
        # rimarrà in esecuzione qui finché non si ferma il server (es. con CTRL+C).
        app.run(host=host, port=port, debug=debug)
    except Exception as e:
        logger.critical(f"Errore critico durante l'avvio del server web: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Il server web è stato fermato.")