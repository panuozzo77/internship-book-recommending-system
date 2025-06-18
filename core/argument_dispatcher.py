# core/argument_dispatcher.py
import argparse
import os

from etl.loader import run_etl, exec_all_etl
#from recommender.engine import ContentBasedRecommender
from recommender.engine import ContentBasedAnnoyRecommender
from recommender.user_profiler import UserProfiler
from utils.logger import LoggerManager  # Your LoggerManager
from core.path_registry import PathRegistry  # Your PathRegistry
from typing import Dict, Any
from webapp.runner import run_web_ui  # Function to run the web UI

logger_manager = LoggerManager()


class ArgumentDispatcher:
    def __init__(self, parsed_args: argparse.Namespace, app_config: Dict[str, Any], registry: PathRegistry):
        """
        Initializes the ArgumentDispatcher.
        Args:
            parsed_args: The Namespace object from argparse.parse_args().
            app_config: The loaded application configuration.
            registry: The application's PathRegistry instance.
        """
        self.args = parsed_args
        self.app_config = app_config
        self.registry = registry

    def dispatch(self) -> None:
        """
        Dispatches actions based on the parsed command-line arguments.
        """
        logger = logger_manager.get_logger()
        action_taken = False

        if self.args.load_etl:
            logger.info("Dispatching: Load all configured ETLs action from CLI.")
            self._dispatch_load_all_configured_etls()
            action_taken = True

        if self.args.specific_etl:
            #self._dispatch_load_specific_etl(self.args.specific_etl)
            action_taken = True

        if self.args.recommend:
            logger.info("Dispatching: Get recommendations action from CLI.")
            self._dispatch_get_recommendations()
            action_taken = True
        
        if self.args.user_profile:
            logger.info("Dispatching: Get recommendations for user profile from CLI.")
            self._dispatch_recommend_for_user_id()
            action_taken = True
        
        if self.args.webui:
            logger.info("Dispatching: Run Web User Interface action from CLI.")
            # La configurazione viene passata alla funzione di avvio
            run_web_ui(self.app_config)
            action_taken = True

        # Add other actions here
        # if self.args.shrink_dataset:
        #     self._dispatch_shrink_dataset(self.args.shrink_dataset)
        #     action_taken = True

        if not action_taken:
            logger.info("No specific CLI action to dispatch (or action not yet implemented in dispatcher).")

    def _dispatch_load_all_configured_etls(self) -> None:
        logger = logger_manager.get_logger()
        """Handles the --load_etl flag for all configured ETLs."""
        logger.info("Dispatching: Loading all configured ETLs action from config file.")

        etl_list = self.app_config.get("etl_list", [])
        etl_configs_base_dir = self.registry.get_path('etl_configs_dir')

        if not etl_list:
            logger.warning("No ETL mapping files listed in 'etl_list' in the application config.")
            return
        if not etl_configs_base_dir:
            logger.error("Path for 'etl_configs_dir' not found in PathRegistry. Cannot locate ETL files.")
            return

        logger.info(f"Found {len(etl_list)} ETL configurations to process: {etl_list}")

        # retrieve the ETL lis files absolute path
        etl_list_paths = [os.path.join(etl_configs_base_dir, etl_file) for etl_file in etl_list]
        exec_all_etl(etl_list_paths, self.app_config, PathRegistry())


    def _dispatch_get_recommendations(self) -> None:
        """Handles the --recommend flag."""
        logger = logger_manager.get_logger()
        
        input_books = self.args.recommend
        top_n = self.args.top_n

        logger.info(f"Attempting to get {top_n} recommendations for: {input_books}")

        try:
            # L'inizializzazione dell'engine è pesante, quindi viene fatta qui
            recommender_engine = ContentBasedAnnoyRecommender()

            # Ottieni le raccomandazioni
            recommendations = recommender_engine.get_recommendations(input_book_titles=input_books, top_n=top_n)

            if recommendations:
                print("\n--- Top Recommendations ---")
                for i, title in enumerate(recommendations):
                    print(f"{i+1}. {title}")
                print("---------------------------\n")
            else:
                logger.warning("Could not generate any recommendations.")

        except Exception as e:
            logger.critical(f"An error occurred during the recommendation process: {e}", exc_info=True)

    def _dispatch_recommend_from_profile(self):
        profile_file_path = self.args.profile_file
        top_n = self.args.top_n
        
        logger = logger_manager.get_logger()
        logger.info("Avvio del processo di raccomandazione basato su profilo utente...")
        
        # 1. Inizializza il motore (carica/costruisce l'indice)
        try:
            recommender = ContentBasedAnnoyRecommender()
        except Exception as e:
            logger.critical(f"Impossibile inizializzare il motore di raccomandazione: {e}")
            return

        # 2. Inizializza il profiler
        profiler = UserProfiler(recommender)
        
        # 3. Crea il profilo utente dal file
        profile_data = profiler.create_weighted_profile_from_file(profile_file_path)
        
        if profile_data is None:
            logger.error("Creazione del profilo fallita. Interruzione del processo.")
            return
            
        user_profile_vector, read_book_indices = profile_data
        
        # 4. Ottieni le raccomandazioni usando il profilo
        # (Dovrai aggiungere un metodo al recommender che accetta un profilo)
        
        # Aggiungiamo un nuovo metodo a `ContentBasedAnnoyRecommender`
        recommendations = recommender.get_recommendations_by_profile(
            user_profile_vector,
            read_book_indices,
            top_n=top_n
        )
        
        if recommendations:
            print("\n--- Top Recommendations Based on Your Profile ---")
            for i, title in enumerate(recommendations):
                print(f"{i+1}. {title}")
            print("--------------------------------------------------\n")
        else:
            logger.warning("Impossibile generare raccomandazioni per il profilo fornito.")


    def _dispatch_recommend_for_user_id(self):
        user_id = self.args.user_profile
        top_n = self.args.top_n
        
        logger = logger_manager.get_logger()
        logger.info(f"Avvio processo di raccomandazione per l'utente: {user_id}")
        
        try:
            recommender = ContentBasedAnnoyRecommender()
            profiler = UserProfiler(recommender)
            
            profile_data = profiler.create_weighted_profile_from_db(user_id)
            
            if profile_data:
                user_profile_vector, read_book_indices = profile_data
                '''
                recommendations = recommender.get_recommendations_by_profile(
                    user_profile_vector,
                    read_book_indices,
                    top_n=top_n
                )
                '''
                print(f'{user_profile_vector}\n{read_book_indices}')
                # ... (stampa i risultati come prima)
            else:
                logger.error(f"Creazione del profilo fallita per l'utente {user_id}.")

        except Exception as e:
            logger.critical(f"Errore critico durante la raccomandazione per l'utente {user_id}: {e}", exc_info=True)

    