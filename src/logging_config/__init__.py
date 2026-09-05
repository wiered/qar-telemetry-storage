"""Configure and initialize application logging.

This module defines a dictionary-based logging configuration and applies it on
import. It also provides a custom formatter that injects ANSI colors into the
log level name for console output.
"""

import logging
from logging.config import dictConfig
from typing import Any

from settings import settings

console_handler_level = settings.logging_level
console_handler_format = settings.logging_format

LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(levelname)s: %(asctime)s %(name)s - %(message)s"},
        "detailed": {
            "format": "%(levelname)s: %(asctime)s %(name)s (%(filename)s:%(lineno)d) - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": console_handler_format,
            "level": console_handler_level,
            "stream": "ext://sys.stdout",
        },
        "file_debug": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "detailed",
            "level": "DEBUG",
            "filename": "debug.log",
            "maxBytes": 1024 * 1024 * 5,
            "backupCount": 3,
            "encoding": "utf8",
        },
        "file_error": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "detailed",
            "level": "ERROR",
            "filename": "error.log",
            "maxBytes": 1024 * 1024 * 5,
            "backupCount": 3,
            "encoding": "utf8",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console", "file_debug", "file_error"],
            "level": "DEBUG",
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
    },
}


class ColoredFormatter(logging.Formatter):
    """Format log records with ANSI colors based on level."""

    green = "\033[0;32m"
    yellow = "\033[1;33m"
    red = "\033[1;31m"
    purple = "\033[0;35m"
    reset = "\033[0m"

    colors = {
        logging.INFO: green,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.DEBUG: purple,
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record and colorize its level name.

        Args:
            record: Log record produced by the logging system.

        Returns:
            Formatted message with a colored level name when a color mapping
            exists for the record level.
        """
        message = super().format(record)
        color = self.colors.get(record.levelno, "")
        if color:
            levelname = f"{record.levelname}"
            colored_level = f"{color}{levelname}{self.reset}"
            message = message.replace(levelname, colored_level)
        return message


def setup_logging() -> None:
    """Apply global logging configuration for the application."""
    logs_dir = settings.data_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    LOGGING_CONFIG["handlers"]["file_debug"]["filename"] = str(logs_dir / "debug.log")
    LOGGING_CONFIG["handlers"]["file_error"]["filename"] = str(logs_dir / "error.log")
    dictConfig(LOGGING_CONFIG)


setup_logging()
