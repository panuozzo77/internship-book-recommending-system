# testing/performance_tester.py

import time
import csv
import logging
from typing import List, Dict, Any, Callable
from collections import Counter

from data_processing.services import BookCreationService, BookUpdateService
from core.utils.LoggerManager import LoggerManager

class PerformanceTester:
    """
    Esegue suite di test per analizzare le performance dei servizi
    di creazione e aggiornamento dei libri.
    """
    def __init__(self, creation_service: BookCreationService, update_service: BookUpdateService):
        self.creation_service = creation_service
        self.update_service = update_service
        self.results: List[Dict[str, Any]] = []
        self.logger = LoggerManager().get_logger()

    def _run_test_case(self, test_type: str, service_method: Callable, kwargs: Dict[str, Any]):
        """Esegue un singolo caso di test e ne registra i risultati."""
        start_time = time.perf_counter()
        
        try:
            result = service_method(**kwargs)
        except Exception as e:
            result = {"status": "CRITICAL_ERROR", "message": str(e)}
        
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        record = {
            "test_type": test_type,
            "input": kwargs,
            "status": result.get("status", "UNKNOWN"),
            "message": result.get("message", ""),
            "execution_time_sec": round(execution_time, 4),
            "book_id": result.get("book_id"),
            "updated_fields": result.get("updated_fields", []),
            "field_sources": result.get("field_sources", {})
        }
        self.results.append(record)
        self.logger.info(f"Test '{test_type}' per {kwargs} completato in {record['execution_time_sec']}s con stato: {record['status']}")

    def run_creation_test_suite(self, test_cases: List[Dict[str, str]]):
        """Esegue una serie di test di creazione di libri."""
        self.logger.info(f"--- AVVIO SUITE DI TEST DI CREAZIONE ({len(test_cases)} casi) ---")
        for i, case in enumerate(test_cases, 1):
            self.logger.info(f"Caso di creazione {i}/{len(test_cases)}: Titolo='{case['title']}', Autore='{case['author_name']}'")
            self._run_test_case(
                test_type="creation",
                service_method=self.creation_service.add_new_book,
                kwargs=case
            )
        self.logger.info("--- SUITE DI TEST DI CREAZIONE COMPLETATA ---")

    def run_update_test_suite(self, created_books_ids: List[str]):
        """Esegue una serie di test di aggiornamento usando gli ID dei libri appena creati."""
        if not created_books_ids:
            self.logger.warning("Nessun libro creato con successo, la suite di test di aggiornamento sarà saltata.")
            return
            
        self.logger.info(f"--- AVVIO SUITE DI TEST DI AGGIORNAMENTO ({len(created_books_ids)} casi) ---")
        for i, book_id in enumerate(created_books_ids, 1):
            identifier = {"book_id": book_id}
            self.logger.info(f"Caso di aggiornamento {i}/{len(created_books_ids)}: ID='{book_id}'")
            self._run_test_case(
                test_type="update",
                service_method=self.update_service.update_book,
                kwargs={"identifier": identifier}
            )
        self.logger.info("--- SUITE DI TEST DI AGGIORNAMENTO COMPLETATA ---")

    def print_summary(self):
        """Stampa un riepilogo statistico dei risultati dei test."""
        if not self.results:
            print("Nessun risultato da analizzare.")
            return

        print("\n" + "="*25 + " RIEPILOGO PERFORMANCE " + "="*25)
        
        total_tests = len(self.results)
        total_time = sum(r['execution_time_sec'] for r in self.results)
        
        status_counts = Counter(r['status'] for r in self.results)
        
        print(f"Test totali eseguiti: {total_tests}")
        print(f"Tempo totale di esecuzione: {total_time:.2f} secondi")
        if total_tests > 0:
            print(f"Tempo medio per operazione: {total_time / total_tests:.2f} secondi")

        print("\nRiepilogo stati:")
        for status, count in status_counts.items():
            percentage = (count / total_tests) * 100
            print(f"- {status}: {count} ({percentage:.1f}%)")

        # Statistiche per tipo di test
        for test_type in ["creation", "update"]:
            type_results = [r for r in self.results if r['test_type'] == test_type]
            if type_results:
                avg_time = sum(r['execution_time_sec'] for r in type_results) / len(type_results)
                print(f"\nTempo medio per '{test_type}': {avg_time:.2f} secondi")

        print("="*72 + "\n")

    def save_results_to_csv(self, filename: str):
        """Salva i risultati dettagliati in un file CSV."""
        if not self.results:
            return
        
        keys = self.results[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.results)
        self.logger.info(f"Risultati dei test salvati in '{filename}'")