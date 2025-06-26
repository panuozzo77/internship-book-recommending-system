# core/path_registry.py
import logging
from typing import Dict, Optional

class PathRegistry:
    _instance = None
    _paths: Dict[str, str] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._paths = {}
        return cls._instance

    def set_path(self, alias: str, path: str) -> None:
        """Register a path with the given alias"""
        self._paths[alias] = path
        logging.debug(f"Path registered: {alias} -> {path}")

    def get_path(self, alias: str) -> Optional[str]:
        """Get a registered path by alias"""
        path = self._paths.get(alias)
        if path is None:
            logging.warning(f"Path not found for alias: {alias}")
        return path

    def all_paths(self) -> Dict[str, str]:
        """Get all registered paths"""
        return dict(self._paths)