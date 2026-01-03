"""
Logging configuration for YouTube metadata fetcher.
Provides both console and file logging with DEBUG level for development.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(
    log_dir: str = "logs",
    log_level: str = None,
    console_level: str = None
) -> logging.Logger:
    """
    Set up logging with both file and console handlers.
    
    Args:
        log_dir: Directory for log files
        log_level: Overall log level (default: DEBUG, or LOG_LEVEL env var)
        console_level: Console output level (default: INFO, or CONSOLE_LOG_LEVEL env var)
    
    Returns:
        Configured logger instance
    """
    # Get levels from env or defaults
    log_level = log_level or os.environ.get("LOG_LEVEL", "DEBUG")
    console_level = console_level or os.environ.get("CONSOLE_LOG_LEVEL", "INFO")
    
    # Create logs directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("youtube_fetcher")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers = []
    
    # Detailed format for file
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Simpler format for console
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler - DEBUG level, rotating by run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(log_dir) / f"fetch_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Also create a latest.log symlink/copy for easy access
    latest_log = Path(log_dir) / "latest.log"
    try:
        if latest_log.exists():
            latest_log.unlink()
        # Use symlink on Unix, copy reference on Windows
        if os.name != 'nt':
            latest_log.symlink_to(log_file.name)
    except (OSError, NotImplementedError):
        pass  # Symlinks might not work everywhere
    
    # Console handler - configurable level
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Log startup info
    logger.info(f"Logging initialized: file={log_file}, level={log_level}")
    logger.debug(f"Console level: {console_level}")
    logger.debug(f"Python version: {sys.version}")
    logger.debug(f"Working directory: {os.getcwd()}")
    
    return logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a child logger for a specific module."""
    base_logger = logging.getLogger("youtube_fetcher")
    if name:
        return base_logger.getChild(name)
    return base_logger


class LogContext:
    """Context manager for logging operation blocks with timing."""
    
    def __init__(self, logger: logging.Logger, operation: str, level: int = logging.DEBUG):
        self.logger = logger
        self.operation = operation
        self.level = level
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.log(self.level, f"START: {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if exc_type:
            self.logger.error(f"FAILED: {self.operation} after {elapsed:.2f}s - {exc_type.__name__}: {exc_val}")
        else:
            self.logger.log(self.level, f"DONE: {self.operation} in {elapsed:.2f}s")
        return False  # Don't suppress exceptions
