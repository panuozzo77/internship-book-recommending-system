import subprocess
import shlex
import re
import os
import logging # Import logging
from typing import List, Optional, Callable, Dict, Union

# from data_processing.book_data_provider_interface import BookDataProvider, BookMetadata
from ..book_data_provider_interface import BookDataProvider, BookMetadata # Adattato
from core.utils.LoggerManager import LoggerManager # Assumendo utils sia a livello di project

# Aggiorna la firma del Callable per usare logging.Logger
CliOutputParser = Callable[[str, logging.Logger], Optional[BookMetadata]]

# Aggiorna la firma per usare logging.Logger
def parse_calibre_opf_output(opf_output: str, logger: logging.Logger) -> Optional[BookMetadata]:
    """Parser specifico per l'output OPF di fetch-ebook-metadata."""
    data: BookMetadata = {}
    
    desc_match = re.search(r'<dc:description.*?>(.*?)</dc:description>', opf_output, re.DOTALL | re.IGNORECASE)
    if desc_match:
        desc_html = desc_match.group(1).strip()
        desc_text = re.sub(r'<[^>]+>', '', desc_html)
        # CORREZIONE: La decodifica delle entità HTML era errata, le stavo ricodificando.
        # Semplice sostituzione per le entità più comuni. Per una soluzione robusta, usare html.unescape.
        desc_text = desc_text.replace('<', '<').replace('>', '>').replace('&', '&').replace('"', '"').replace(''', "'").replace(''', "'")
        data['description'] = desc_text.strip()

    # CORREZIONE: Regex per page_count, i backslash erano doppi.
    pages_match = re.search(r'<meta\s+name="calibre:pages"\s+content="(\d+)"\s*/>', opf_output, re.IGNORECASE)
    if pages_match:
        try:
            data['page_count'] = int(pages_match.group(1))
        except ValueError:
            logger.warning(f"CalibreParser: page_count non valido: {pages_match.group(1)}")
            
    subject_tags = re.findall(r'<dc:subject.*?>(.*?)</dc:subject>', opf_output, re.IGNORECASE)
    if subject_tags:
        genres = [s.strip().lower() for s in subject_tags if s.strip()]
        if genres:
            data['genres'] = sorted(list(set(genres))) # _normalize_genres verrà chiamato dal chiamante
    return data if data else None

# Parser per lo script Rust (firma aggiornata per logging.Logger)
def parse_goodreads_rust_scraper_output(output: str, logger: logging.Logger) -> Optional[BookMetadata]:
    """
    Parser aggiornato e preciso per l'output reale dello scraper Rust di Goodreads.
    Estrae i dati basandosi sulla struttura `BookMetadata { ... }`.
    """
    data: BookMetadata = {}
    logger.debug("GoodreadsRustParser: Parsing new output structure...")

    # Funzione helper per estrarre stringhe escapate da Rust
    def unescape(s: str) -> str:
        return s.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t').replace('\\\\', '\\')

    # --- Titolo e Sottotitolo ---
    title_match = re.search(r'title: "(.*?)",', output, re.DOTALL)
    subtitle_match = re.search(r'subtitle: Some\(\s*"(.*?)"\s*,\s*\),', output, re.DOTALL)
    
    title_parts = []
    if title_match:
        title_parts.append(unescape(title_match.group(1)))
    if subtitle_match:
        title_parts.append(unescape(subtitle_match.group(1)))
    
    if title_parts:
        data['title'] = ": ".join(title_parts)

    # --- Descrizione ---
    if desc_match := re.search(r'description: Some\(\s*"(.*?)"\s*,\s*\),', output, re.DOTALL):
        data['description'] = unescape(desc_match.group(1))

    # --- Conteggio Pagine ---
    if pages_match := re.search(r'page_count: Some\(\s*(\d+)\s*,\s*\),', output):
        data['page_count'] = int(pages_match.group(1))

    # --- Anno di Pubblicazione ---
    if date_match := re.search(r'publication_date: Some\(\s*(\d{4})-\d{2}-\d{2}T.*?\),', output):
        data['publication_year'] = int(date_match.group(1))

    # --- Editore ---
    if pub_match := re.search(r'publisher: Some\(\s*"(.*?)"\s*,\s*\),', output, re.DOTALL):
        data['publisher'] = unescape(pub_match.group(1))

    # --- Autori (estratti dai "contributors") ---
    contributors_block_match = re.search(r'contributors: \[\s*(.*?)\s*\],', output, re.DOTALL)
    if contributors_block_match:
        # Trova tutti i contributori che hanno il ruolo "Author"
        authors_found = re.findall(r'BookContributor \{\s*name: "(.*?)",\s*role: "Author",\s*\}', contributors_block_match.group(1))
        if authors_found:
            data['authors'] = [unescape(name) for name in authors_found]
            # Nota: non abbiamo un ID autore separato qui, quindi non impostiamo provider_specific_author_id

    # --- Generi ---
    genres_block_match = re.search(r'genres: \[\s*(.*?)\s*\],', output, re.DOTALL)
    if genres_block_match:
        # Estrai tutte le stringhe tra virgolette nel blocco genres
        raw_genres = re.findall(r'"(.*?)"', genres_block_match.group(1))
        if raw_genres:
            data['genres'] = [unescape(g) for g in raw_genres]
            # Usiamo i generi anche come stima per i popular_shelves se non disponibili altrove
            if 'popular_shelves' not in data:
                data['popular_shelves'] = [{"name": g.lower(), "count": "0"} for g in data['genres']]

    # --- Serie ---
    series_block_match = re.search(r'series: Some\(\s*BookSeries \{\s*(.*?)\s*\}\s*,\s*\),', output, re.DOTALL)
    if series_block_match:
        series_content = series_block_match.group(1)
        series_title_match = re.search(r'title: "(.*?)"', series_content)
        if series_title_match:
            # L'ID della serie non è esposto, quindi ne generiamo uno basato sul titolo per coerenza
            series_name = unescape(series_title_match.group(1))
            data['series'] = {
                # Non avendo un ID, non possiamo impostarlo qui. Il repository se ne occuperà.
                "name": series_name
            }

    # ID del libro (non sembra esserci nell'output, usiamo ISBN come fallback se disponibile)
    if isbn_match := re.search(r'isbn: Some\(\s*"(.*?)"\s*,\s*\),', output, re.DOTALL):
        data['provider_specific_id'] = f"GR_ISBN_{unescape(isbn_match.group(1))}"
    
    if data:
        logger.info(f"GoodreadsRustParser: Parsed data successfully. Fields: {list(data.keys())}")
        return data
    else:
        logger.info("GoodreadsRustParser: No useful data parsed from the output.")
        return None


class CliBookProvider(BookDataProvider):
    def __init__(self, 
                 provider_name: str,
                 base_command_args: List[str],
                 output_parser: CliOutputParser,
                 title_option: Optional[Union[str, List[str]]] = None,
                 author_option: Optional[Union[str, List[str]]] = None,
                 pass_authors_individually: bool = False,
                 cwd: Optional[str] = None,
                 timeout: int = 30):
        self.logger: logging.Logger = LoggerManager().get_logger() # Ottiene il logger configurato
        self._provider_name = provider_name
        self.base_command_args = base_command_args
        self.title_option = title_option
        self.author_option = author_option
        self.pass_authors_individually = pass_authors_individually
        self.output_parser = output_parser
        self.cwd = os.path.expanduser(cwd) if cwd else None
        self.timeout = timeout

    def get_name(self) -> str:
        return self._provider_name

    def _build_command(self, title: str, authors: List[str]) -> List[str]:
        cmd_list = list(self.base_command_args)
        if self.title_option:
            if isinstance(self.title_option, list): cmd_list.extend(self.title_option)
            else: cmd_list.append(self.title_option)
            cmd_list.append(title)
        else:
            cmd_list.append(title)

        if authors:
            if self.author_option:
                if self.pass_authors_individually:
                    for author in authors:
                        if isinstance(self.author_option, list): cmd_list.extend(self.author_option)
                        else: cmd_list.append(self.author_option)
                        cmd_list.append(author)
                else:
                    authors_str = ", ".join(authors)
                    if isinstance(self.author_option, list): cmd_list.extend(self.author_option)
                    else: cmd_list.append(self.author_option)
                    cmd_list.append(authors_str)
            else:
                if authors: cmd_list.append(authors[0]) # Semplificazione per tool che prendono 1 autore posizionale
        return cmd_list

    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        cmd_list = self._build_command(title, authors)
        self.logger.debug(f"{self.get_name()}: Executing command: {' '.join(shlex.quote(str(s)) for s in cmd_list)} in CWD: {self.cwd}")

        try:
            result = subprocess.run(
                cmd_list, capture_output=True, text=True, timeout=self.timeout,
                encoding='utf-8', errors='replace', shell=False, cwd=self.cwd
            )

            # Gestione del fallimento del comando
            if result.returncode != 0:
                # Gestione specifica per il panic "Book not found" di Rust
                if result.returncode == 101 and "Book not found" in result.stderr:
                    self.logger.info(f"{self.get_name()}: Book '{title}' not found by the Rust scraper (handled panic).")
                    return None
                
                # Per tutti gli altri errori, logga un warning
                self.logger.warning(
                    f"{self.get_name()}: Command failed for '{title}' (code {result.returncode}).\n"
                    f"  CWD: {self.cwd}\n  Stderr: {result.stderr.strip()}"
                )
                return None

            output_to_parse = result.stdout
            if not output_to_parse.strip():
                self.logger.info(f"{self.get_name()}: No stdout from command for '{title}'.")
                return None

            parsed_data = self.output_parser(output_to_parse, self.logger)
            
            if parsed_data:
                # Logga quali campi sono stati restituiti dal parser
                self.logger.info(f"{self.get_name()}: Provider returning data with fields: {list(parsed_data.keys())}")
            
            return parsed_data # <<< RESTITUISCE TUTTI I DATI PARSATI

        except FileNotFoundError:
            self.logger.error(f"{self.get_name()}: Command '{cmd_list[0]}' not found. Is it in your PATH?")
            return None
        except subprocess.TimeoutExpired:
            self.logger.error(f"{self.get_name()}: Timeout ({self.timeout}s) executing command for '{title}'.")
            return None
        except Exception as e:
            self.logger.error(f"{self.get_name()}: Unexpected error executing command for '{title}': {e}", exc_info=True)
            return None