"""
Logging configuration for YouTube metadata fetcher.
Provides both console and file logging with DEBUG level for development.

Supports thread-local channel context for parallel processing - each log
line is automatically prefixed with the current channel identifier.

Configuration can be set via config/channels.yaml settings section or environment variables.
"""

import logging
import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional


def _get_config_safe():
    """
    Safely get config without causing circular import issues.

    Returns None if config cannot be loaded (e.g., during early initialization).
    """
    try:
        from config import get_config
        return get_config()
    except Exception:
        return None

# Thread-local storage for current channel context
_channel_context = threading.local()


def set_channel_context(channel_id: str) -> None:
    """Set the current channel context for this thread's log messages."""
    _channel_context.channel = channel_id


def get_channel_context() -> Optional[str]:
    """Get the current channel context for this thread."""
    return getattr(_channel_context, 'channel', None)


def clear_channel_context() -> None:
    """Clear the channel context for this thread."""
    _channel_context.channel = None


class ChannelContextFormatter(logging.Formatter):
    """Formatter that includes thread-local channel context in log messages."""

    def format(self, record):
        # Add channel prefix if context is set
        # Use a custom attribute to avoid double-prefixing when multiple handlers format the same record
        channel = get_channel_context()
        if channel and not getattr(record, '_channel_prefixed', False):
            # Use short form: @handle -> [handle], UCxxxx -> [UCxx...]
            if channel.startswith('@'):
                short = channel[1:13]  # Remove @ and truncate
            elif channel.startswith('UC'):
                short = channel[:8]  # First 8 chars of channel ID
            else:
                short = channel[:10]
            record.msg = f"[{short}] {record.msg}"
            record._channel_prefixed = True
        return super().format(record)


def setup_logging(
    log_dir: Optional[str] = None,
    log_level: Optional[str] = None,
    console_level: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging with both file and console handlers.

    Args:
        log_dir: Directory for log files (default: from config, env, or "logs")
        log_level: Overall log level (default: from config, env, or "DEBUG")
        console_level: Console output level (default: from config, env, or "INFO")

    Returns:
        Configured logger instance
    """
    # Try to get values from config first, then env, then defaults
    cfg = _get_config_safe()

    if log_dir is None:
        if cfg:
            log_dir = cfg.log_dir
        else:
            log_dir = os.environ.get("LOG_DIR", "logs")

    if log_level is None:
        if cfg:
            log_level = cfg.log_level
        else:
            log_level = os.environ.get("LOG_LEVEL", "DEBUG")

    if console_level is None:
        if cfg:
            console_level = cfg.console_log_level
        else:
            console_level = os.environ.get("CONSOLE_LOG_LEVEL", "INFO")
    
    # Create logs directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("youtube_fetcher")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers = []
    
    # Detailed format for file (with channel context)
    file_formatter = ChannelContextFormatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Simpler format for console (with channel context)
    console_formatter = ChannelContextFormatter(
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
    except (OSError, NotImplementedError) as e:
        # Symlinks might not work everywhere (e.g., some filesystems, Windows)
        logger.debug(f"Could not create latest.log symlink: {e}")
    
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


def get_logger(name: Optional[str] = None) -> logging.Logger:
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
