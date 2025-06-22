import csv
import json
import subprocess
import time
import re

def fetch_metadata_with_calibre(title: str, author: str):
    """Esegue fetch-ebook-metadata e restituisce i dati estratti."""
    print(f"-> Ricerca per: '{title}' di '{author}'...")
    command = ['fetch-ebook-metadata', '--opf', '-t', title]
    if author:
        command.extend(['-a', author])

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, encoding='utf-8')

        if result.returncode != 0:
            print(f"   ...Libro non trovato o errore Calibre (codice {result.returncode}).")
            return None

        output = result.stdout
        if not output:
            return None

        # Parsing semplice dell'output OPF
        data = {}
        # Descrizione
        desc_match = re.search(r'<dc:description>(.*?)</dc:description>', output, re.DOTALL)
        if desc_match:
            desc_html = desc_match.group(1).strip()
            data['description'] = re.sub('<[^<]+?>', '', desc_html) # Rimuove tag HTML
        
        # Pagine (tag non standard di Calibre)
        pages_match = re.search(r'<meta name="calibre:pages" content="(\d+)"', output)
        if pages_match:
            data['page_count'] = int(pages_match.group(1))

        return data if data else None

    except Exception as e:
        print(f"   ...Eccezione durante la chiamata a Calibre: {e}")
        return None

def process_file(input_path: str, output_path: str):
    """Legge il file di input, esegue il fetch e scrive l'output."""
    enriched_books = []
    
    try:
        with open(input_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            books_to_process = list(reader)

        print(f"Trovati {len(books_to_process)} libri da processare in '{input_path}'.")

        for i, row in enumerate(books_to_process):
            print(f"\n[{i+1}/{len(books_to_process)}]")
            book_id = row['book_id']
            title = row['book_title']
            author = row['author_name']

            metadata = fetch_metadata_with_calibre(title, author)
            
            # Crea il record di output
            output_record = {'book_id': book_id}
            if metadata:
                print("   ...Metadati trovati!")
                output_record.update(metadata)
                output_record['status'] = 'PROCESSED_SUCCESS'
            else:
                output_record['status'] = 'NOT_FOUND'

            enriched_books.append(output_record)
            time.sleep(1) # Pausa per non sovraccaricare le API esterne

        # Scrive tutti i risultati in un file JSON
        with open(output_path, 'w', encoding='utf-8') as f_out:
            json.dump(enriched_books, f_out, indent=2, ensure_ascii=False)
        
        print(f"\nProcesso completato. Risultati salvati in '{output_path}'.")

    except FileNotFoundError:
        print(f"Errore: File di input '{input_path}' non trovato.")

if __name__ == '__main__':
    INPUT_CSV_PATH = "output/books_to_process.csv"
    OUTPUT_JSON_PATH = "enriched_book_data.json"
    process_file(INPUT_CSV_PATH, OUTPUT_JSON_PATH)