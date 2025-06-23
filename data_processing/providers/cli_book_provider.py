import subprocess
import shlex
import re
import os
import logging # Import logging
from typing import List, Optional, Callable, Dict, Union

# from data_processing.book_data_provider_interface import BookDataProvider, BookMetadata
from ..book_data_provider_interface import BookDataProvider, BookMetadata # Adattato
from utils.logger import LoggerManager # Assumendo utils sia a livello di project

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
    Parser per l'output testuale dello script Rust che stampa una struct BookMetadata.
    """
    data: BookMetadata = {}
    logger.debug(f"GoodreadsRustParser: Inizio parsing output: {output[:300]}...")

    desc_match = re.search(r'description: Some\(\s*"( ((?:\\"|[^"])*) )"\s*,\s*\),', output, re.DOTALL)
    if desc_match:
        description_raw = desc_match.group(1)
        data['description'] = description_raw.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t').replace('\\\\', '\\')
        logger.debug(f"GoodreadsRustParser: Descrizione trovata: {data['description'][:50]}...")
    else:
        logger.debug("GoodreadsRustParser: Descrizione non trovata.")

    pages_match = re.search(r'page_count: Some\(\s*(\d+)\s*,\s*\),', output)
    if pages_match:
        try:
            data['page_count'] = int(pages_match.group(1))
            logger.debug(f"GoodreadsRustParser: Page count trovato: {data['page_count']}")
        except ValueError:
            logger.warning(f"GoodreadsRustParser: page_count non valido nel testo: {pages_match.group(1)}")
    else:
        logger.debug("GoodreadsRustParser: Page count non trovato.")

    genres_block_match = re.search(r'genres: \[\s*(.*?)\s*\],', output, re.DOTALL)
    if genres_block_match:
        genres_content = genres_block_match.group(1).strip()
        if genres_content:
            raw_genres = re.findall(r'"(.*?)"', genres_content)
            cleaned_genres = [g.replace('\\"', '"').replace('\\\\', '\\') for g in raw_genres if g.strip()]
            if cleaned_genres:
                data['genres'] = cleaned_genres # _normalize_genres verrà chiamato dal chiamante
                logger.debug(f"GoodreadsRustParser: Generi trovati: {data['genres']}")
            else:
                logger.debug("GoodreadsRustParser: Blocco generi trovato ma nessun genere valido estratto.")
        else:
            logger.debug("GoodreadsRustParser: Blocco generi trovato ma vuoto.")
    else:
        logger.debug("GoodreadsRustParser: Blocco generi non trovato.")
    
    if data:
        logger.info(f"GoodreadsRustParser: Dati parsati con successo: {list(data.keys())}")
        return data
    else:
        logger.info("GoodreadsRustParser: Nessun dato utile parsato.")
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
        author_for_rust_script = authors[0] if authors else ""
        
        cmd_list: List[str]
        if self._provider_name == "GoodreadsRustScraper":
             cmd_list = list(self.base_command_args)
             cmd_list.append(title)
             if author_for_rust_script:
                 cmd_list.append(author_for_rust_script)
        else:
            cmd_list = self._build_command(title, authors)

        self.logger.debug(f"{self.get_name()}: Esecuzione comando: {' '.join(shlex.quote(str(s)) for s in cmd_list)} in CWD: {self.cwd or os.getcwd()}")

        try:
            result = subprocess.run(
                cmd_list, capture_output=True, text=True, timeout=self.timeout,
                encoding='utf-8', errors='replace', shell=False, cwd=self.cwd
            )

            actual_output = result.stdout
            if "BookMetadata {" in result.stdout: # Per output Rust struct
                actual_output = result.stdout[result.stdout.find("BookMetadata {"):]
            elif result.stdout.strip().startswith("{"): # Per output JSON
                 actual_output = result.stdout[result.stdout.find("{"):]

            if result.returncode != 0:
                self.logger.warning(
                    f"{self.get_name()}: Comando per '{title}' terminato con errore (codice {result.returncode}).\n"
                    f"  Comando: {' '.join(shlex.quote(str(s)) for s in cmd_list)}\n"
                    f"  CWD: {self.cwd}\n  Stderr: {result.stderr.strip()}\n"
                    f"  Stdout (parte utile): {actual_output[:500].strip()}..."
                )
                return None
            
            if not actual_output.strip():
                self.logger.info(f"{self.get_name()}: Nessun output utile (dopo pulizia) da stdout per '{title}'. Full stdout: {result.stdout.strip()}")
                return None

            # Passa self.logger (che è logging.Logger) al parser
            parsed_data = self.output_parser(actual_output, self.logger)

            if parsed_data:
                final_data: BookMetadata = {}
                for key_typed, value_typed in parsed_data.items():
                    key = str(key_typed) # Assicura che la chiave sia una stringa
                    value = value_typed
                    if not existing_data or key not in existing_data:
                        if key == "description" and isinstance(value, str) and value.strip():
                            final_data["description"] = value
                        elif key == "page_count" and isinstance(value, int) and value > 0:
                            final_data["page_count"] = value
                        elif key == "genres" and isinstance(value, list) and value:
                            # La normalizzazione ora avviene nel BookDataProvider base
                            final_data["genres"] = self._normalize_genres(value) 
                
                if final_data:
                    self.logger.info(f"{self.get_name()}: Dati trovati per '{title}': {list(final_data.keys())}")
                    return final_data
                else:
                    self.logger.info(f"{self.get_name()}: Nessun nuovo dato utile (o già esistente) trovato da CLI per '{title}'.")
                    return None
            else:
                self.logger.info(f"{self.get_name()}: Parser non ha estratto dati utili per '{title}'.")
                return None

        except FileNotFoundError:
            self.logger.error(f"{self.get_name()}: Comando '{cmd_list[0]}' non trovato. Assicurati sia installato e nel PATH.")
            return None
        except subprocess.TimeoutExpired:
            self.logger.error(f"{self.get_name()}: Timeout ({self.timeout}s) durante esecuzione comando per '{title}'.")
            return None
        except Exception as e:
            self.logger.error(f"{self.get_name()}: Errore imprevisto esecuzione comando per '{title}': {e}", exc_info=True)
            return None