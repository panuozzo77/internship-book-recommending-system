import os
import requests
import time
import logging
import re
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv

try:
    from ..book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.LoggerManager import LoggerManager
except ImportError:
    from book_data_provider_interface import BookDataProvider, BookMetadata
    from core.utils.LoggerManager import LoggerManager

class GoogleBooksProvider(BookDataProvider):
    API_URL = "https://www.googleapis.com/books/v1/volumes"

    def __init__(self, api_key: Optional[str] = None):
        self.logger: logging.Logger = LoggerManager().get_logger()
        if api_key:
            self.api_key = api_key
        else:
            load_dotenv()
            self.api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
        
        if not self.api_key:
            self.logger.warning("GOOGLE_BOOKS_API_KEY not found. GoogleBooksProvider will be disabled.")
        self.rate_limit_delay = 1.0

    def get_name(self) -> str:
        return "GoogleBooks"

    def fetch_data(self, title: str, authors: List[str], existing_data: Optional[BookMetadata] = None) -> Optional[BookMetadata]:
        if not self.api_key:
            return None

        query_parts = [f"intitle:{title}"]
        if authors:
            query_parts.append(f"inauthor:{' '.join(authors)}")
        
        query = "+".join(query_parts)
        params = {'q': query, 'key': self.api_key, 'maxResults': 1, 'printType': 'books'}
        
        self.logger.info(f"{self.get_name()}: Searching with query: {query}")
        
        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(self.API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{self.get_name()}: API error for '{title}': {e}")
            return None
        
        if not data.get("items"):
            self.logger.info(f"{self.get_name()}: No results for '{title}'.")
            return None

        item = data["items"][0]
        volume_info = item.get("volumeInfo", {})
        result: BookMetadata = {"provider_specific_id": f"GOOG_{item.get('id')}"}
        
        result["title"] = volume_info.get("title")
        result["description"] = volume_info.get("description")
        
        if volume_info.get("pageCount"):
            result["page_count"] = int(volume_info["pageCount"])
        
        if volume_info.get("publisher"):
            result["publisher"] = volume_info["publisher"]
        
        if volume_info.get("publishedDate"):
            year_match = re.search(r'\b(\d{4})\b', volume_info["publishedDate"])
            if year_match:
                result["publication_year"] = int(year_match.group(1))

        if volume_info.get("authors"):
            result["authors"] = volume_info["authors"]
            
        if volume_info.get("categories"):
            result["genres"] = self._normalize_genres(volume_info["categories"])

        if result:
            self.logger.info(f"{self.get_name()}: Data found for '{title}': {list(result.keys())}")
            return result
        
        return None