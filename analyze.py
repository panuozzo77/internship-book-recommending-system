import csv
from collections import defaultdict

def analyze_book_data(file_goodreads, file_others):
    """
    Analizza i dati dei libri da due file CSV confrontando
    GoodreadsRustScraper con altri provider.

    Args:
        file_goodreads (str): Path al file CSV con dati da GoodreadsRustScraper.
        file_others (str): Path al file CSV con dati da altri provider.

    Returns:
        dict: Un dizionario contenente i risultati dell'analisi.
    """

    # Funzione di utilità per caricare i dati dai file CSV
    def load_data(filename):
        data = []
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        return data

    # Carica i dati dai file
    data_goodreads = load_data(file_goodreads)
    data_others = load_data(file_others)

    # --- Analisi delle operazioni di creazione ---

    # Funzione per calcolare le statistiche di creazione
    def calculate_creation_stats(data):
        total_creations = len(data)
        successful_creations = sum(1 for row in data if row['status'] == 'SUCCESS')
        failed_creations = total_creations - successful_creations
        success_rate = (successful_creations / total_creations) * 100 if total_creations else 0
        failed_books = [(row['input'], row['message']) for row in data if row['status'] != 'SUCCESS']
        average_creation_time = sum(float(row['execution_time_sec']) for row in data if row['status'] == 'SUCCESS') / successful_creations if successful_creations else 0
        return {
            'total_creations': total_creations,
            'successful_creations': successful_creations,
            'failed_creations': failed_creations,
            'success_rate': success_rate,
            'failed_books': failed_books,
            'average_creation_time': average_creation_time
        }

    # Calcola le statistiche di creazione per entrambi i file
    creation_stats_goodreads = calculate_creation_stats(data_goodreads)
    creation_stats_others = calculate_creation_stats(data_others)

    # --- Analisi dei generi ---

    def analyze_genre_sources(data):
        genre_sources = defaultdict(int)
        for row in data:
            field_sources = eval(row['field_sources'])  # Converti la stringa in dizionario
            if 'genres' in field_sources:
                source = field_sources['genres']
                genre_sources[source] += 1
        return genre_sources

    # Analizza le fonti dei generi per entrambi i file
    genre_sources_goodreads = analyze_genre_sources(data_goodreads)
    genre_sources_others = analyze_genre_sources(data_others)

    # --- Analisi dei tempi di esecuzione e della copertura ---

    def analyze_execution_time_vs_coverage(data):
        provider_times = defaultdict(lambda: {'total_time': 0, 'count': 0})
        for row in data:
            if row['status'] == 'SUCCESS' and row['field_sources']:
                field_sources = eval(row['field_sources'])
                if 'genres' in field_sources:  #consideriamo solo i tempi dove ci sono i generi
                    provider = field_sources['genres']
                    provider_times[provider]['total_time'] += float(row['execution_time_sec'])
                    provider_times[provider]['count'] += 1
        avg_provider_times = {
            provider: data['total_time'] / data['count']
            for provider, data in provider_times.items() if data['count'] > 0
        }
        return avg_provider_times

    # Esegue l'analisi dei tempi di esecuzione vs copertura
    execution_time_vs_coverage_goodreads = analyze_execution_time_vs_coverage(data_goodreads)
    execution_time_vs_coverage_others = analyze_execution_time_vs_coverage(data_others)

    # --- Identificazione dei provider integrativi (esempio) ---
    def identify_integrative_providers(data_goodreads, data_others):
        integrative_providers = defaultdict(int)
        
        # Crea un set di titoli di libri da Goodreads per riferimento
        goodreads_titles = {row['input'].split(',')[0].split(':')[1].strip().replace("'", "").replace("{", ""): row for row in data_goodreads if row['status'] == 'SUCCESS'}

        for row in data_others:
            if row['status'] == 'SUCCESS':
                book_title = row['input'].split(',')[0].split(':')[1].strip().replace("'", "").replace("{", "")
                if book_title in goodreads_titles:
                    # Verifica se OpenLibrary fornisce 'popular_shelves' e GoogleBooks non lo fa
                    goodreads_shelves = eval(goodreads_titles[book_title]['field_sources']).get('popular_shelves', None)
                    others_shelves = eval(row['field_sources']).get('popular_shelves', None)

                    if goodreads_shelves is None and others_shelves == 'OpenLibrary':
                        integrative_providers['OpenLibrary'] += 1

        return integrative_providers

    # Identifica i provider integrativi
    integrative_providers = identify_integrative_providers(data_goodreads, data_others)

    # --- Restituzione dei risultati ---

    return {
        'creation_stats_goodreads': creation_stats_goodreads,
        'creation_stats_others': creation_stats_others,
        'genre_sources_goodreads': genre_sources_goodreads,
        'genre_sources_others': genre_sources_others,
        'execution_time_vs_coverage_goodreads': execution_time_vs_coverage_goodreads,
        'execution_time_vs_coverage_others': execution_time_vs_coverage_others,
        'integrative_providers': integrative_providers
    }


# Esempio di utilizzo
file_goodreads = 'performance_results_llm.csv'  # Sostituisci con il tuo filename
file_others = 'performance_results_llm_others.csv'  # Sostituisci con il tuo filename

results = analyze_book_data(file_goodreads, file_others)

# Stampa i risultati formattati
print("--- Creazione Statistiche (GoodreadsRustScraper) ---")
for key, value in results['creation_stats_goodreads'].items():
    print(f"{key}: {value}")

print("\n--- Creazione Statistiche (Others) ---")
for key, value in results['creation_stats_others'].items():
    print(f"{key}: {value}")

print("\n--- Fonti dei generi (GoodreadsRustScraper) ---")
for source, count in results['genre_sources_goodreads'].items():
    print(f"{source}: {count}")

print("\n--- Fonti dei generi (Others) ---")
for source, count in results['genre_sources_others'].items():
    print(f"{source}: {count}")

print("\n--- Tempi di esecuzione vs Copertura (GoodreadsRustScraper) ---")
for provider, avg_time in results['execution_time_vs_coverage_goodreads'].items():
    print(f"{provider}: {avg_time:.2f} sec")

print("\n--- Tempi di esecuzione vs Copertura (Others) ---")
for provider, avg_time in results['execution_time_vs_coverage_others'].items():
    print(f"{provider}: {avg_time:.2f} sec")

print("\n--- Provider Integrativi ---")
for provider, count in results['integrative_providers'].items():
    print(f"{provider}: {count}")