# utils/logger.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _logger: Optional[logging.Logger] = None
    _main_logger_name: str = "AppLogger"  # Keep track of the main logger's name
    _initialized: bool = False
    _pre_init_logger_name: str = "pre_init_logger_DO_NOT_USE_DIRECTLY"  # Unique name for pre_init

    def __new__(cls) -> 'LoggerManager':
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        # Get/create the pre_init logger and store it.
        # It will be replaced or its handlers removed by setup_logger.
        self._logger = self._configure_pre_init_logger()
        self._initialized = True

    def _configure_pre_init_logger(self) -> logging.Logger:
        """
        Configures and returns a basic logger for the pre-initialization phase.
        This logger should not propagate to avoid issues when the main logger is set up.
        """
        # Use a distinct name for the pre-init logger
        logger = logging.getLogger(self._pre_init_logger_name)

        # Only configure if it hasn't been configured before (e.g., multiple LoggerManager() calls)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            # CRITICAL: Prevent pre_init from propagating to root,
            # which might get configured by basicConfig later or by other libraries.
            logger.propagate = False

            handler = logging.StreamHandler()  # Log pre-init to console
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - PRE-INIT - %(message)s")
            )
            logger.addHandler(handler)
            # logger.debug(f"Pre-init logger '{self._pre_init_logger_name}' configured.")
        return logger

    def setup_logger(self,
                     name: str,
                     log_file: Optional[str] = None,
                     level: str = "INFO",
                     log_format: Optional[str] = None  # Added log_format parameter
                     ) -> None:
        """
        Configure the main application logger.
        This method configures the logger specified by 'name'.
        It also attempts to clean up the pre-init logger's handlers.
        """
        self._main_logger_name = name  # Store the name of the main logger
        main_logger = logging.getLogger(self._main_logger_name)
        main_logger.setLevel(level.upper())

        # IMPORTANT: Prevent this logger from propagating messages to the root logger.
        # This is often the key to stopping duplicate logs if the root logger also has handlers.
        main_logger.propagate = False

        # Clear any existing handlers from THIS specific logger
        if main_logger.hasHandlers():
            main_logger.handlers.clear()

        # Determine the formatter
        formatter_string = log_format if log_format else "%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s"
        formatter = logging.Formatter(formatter_string)

        # Add console handler to the main logger
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        main_logger.addHandler(console_handler)

        # Add file handler to the main logger if path is provided
        if log_file:
            log_path = Path(log_file)
            try:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                file_handler = RotatingFileHandler(
                    log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                main_logger.addHandler(file_handler)
            except Exception as e:
                # If file handler fails, log to console via the already added console_handler
                main_logger.error(f"Failed to setup file handler for {log_file}: {e}", exc_info=True)

        # Update the LoggerManager's primary logger instance
        self._logger = main_logger

        # Clean up the pre_init logger's handlers if it exists and is different
        # This step helps ensure the pre_init logger stops outputting after main setup.
        pre_init_instance = logging.getLogger(self._pre_init_logger_name)
        if pre_init_instance and pre_init_instance.name != self._main_logger_name:
            if pre_init_instance.hasHandlers():
                # main_logger.debug(f"Clearing handlers from pre-init logger: {self._pre_init_logger_name}")
                pre_init_instance.handlers.clear()
            # Optionally, disable the pre_init logger entirely if it won't be used again
            # pre_init_instance.disabled = True

        self._logger.info(
            f"Logger '{self._main_logger_name}' configured. Level: {level}. Format: '{formatter_string}'. File: {log_file if log_file else 'None'}.")

    def get_logger(self) -> logging.Logger:
        """
        Return the currently configured logger instance.
        """
        if self._logger is None:  # Should ideally be set by __init__ or setup_logger
            # Fallback, re-configure pre-init if something went very wrong
            # This ensures get_logger() always returns a logger.
            return self._configure_pre_init_logger()
        return self._logger