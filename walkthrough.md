# utublr: YouTube Channel Analytics — Code Walkthrough

*2026-03-16T01:36:49Z by Showboat 0.6.1*
<!-- showboat-id: e3fdd320-5d67-42de-9b1a-9a48ef2ee95d -->

## Section 1: Introduction

utublr is a YouTube Channel Analytics Tracker. It monitors a configured list of YouTube channels over time, periodically fetching:

- **Channel metadata** (title, description, subscriber/view counts)
- **Video metadata and stats** (title, description, views, likes, comments counts)
- **Comments** (top-level comment threads plus replies)
- **Transcripts** (auto-generated or manual captions)

All data is persisted to a cloud database — either **Turso** (cloud SQLite via libSQL) or **PostgreSQL** — for trend analysis and reporting. The main fetcher runs automatically via **GitHub Actions four times per day** (midnight, 6am, noon, 6pm UTC).

The module dependency graph flows like this:

    fetch.py  (orchestration)
      ├─ config.py      (configuration)
      ├─ logger.py      (logging)
      ├─ database.py    (storage)
      ├─ youtube_api.py (YouTube Data API v3 client)
      └─ quota.py       (API quota tracking)

Let's start by looking at the project layout.

```bash
find . -name '*.py' | grep -v __pycache__ | sort
```

```output
./scripts/__init__.py
./scripts/analyse.py
./scripts/config.py
./scripts/database.py
./scripts/db_maintenance.py
./scripts/fetch.py
./scripts/local/__init__.py
./scripts/local/fetch_transcripts.py
./scripts/local/test_transcript.py
./scripts/logger.py
./scripts/optimize_database.py
./scripts/optimize_database_postgres.py
./scripts/quota.py
./scripts/scheduled_optimization.py
./scripts/supabase_optimization_workflow.py
./scripts/sync_database.py
./scripts/youtube_api.py
```

```bash
cat pyproject.toml
```

```output
[project]
name = "utublr"
version = "0.1.0"
description = "YouTube Channel Analytics Tracker"
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
    "google-api-python-client>=2.100.0,<2.200.0",
    "youtube-transcript-api>=0.6.0,<2.0.0",
    "requests>=2.28.0,<3.0.0",
    "libsql>=0.1.0,<0.2.0",
    "pyyaml>=6.0,<7.0",
    "psycopg[binary]>=3.1.0,<3.4.0",
    "protobuf>=5.28.0,<6.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["scripts"]
```

## Section 2: Configuration — `scripts/config.py` + `config/channels.yaml`

Configuration follows a **three-level priority** hierarchy:

    Environment Variables  >  channels.yaml  >  Hard-coded defaults

A single `Config` dataclass holds every setting. The `get_config()` singleton is imported everywhere so the whole program shares one consistent configuration object without passing it around.

The DEFAULTS dict at the top of `config.py` shows every tunable knob and its factory value.

```bash
sed -n '1,70p' scripts/config.py
```

```output
"""
Centralized configuration management for YouTube metadata fetcher.

Configuration is loaded from multiple sources with the following priority:
1. Environment variables - highest priority (standard for deployments/CI)
2. Config file (channels.yaml settings section) - defaults for development
3. Default values - lowest priority

Secrets (auth tokens, API keys) ALWAYS come from environment variables for security.

Usage:
    from config import get_config

    config = get_config()
    db_url = config.database_url
    max_retries = config.api_max_retries
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# Default configuration values
DEFAULTS = {
    # Database settings
    "database_backend": "turso",
    "database_url": "file:data/youtube.db",

    # Logging settings
    "log_dir": "logs",
    "log_level": "DEBUG",
    "console_log_level": "INFO",

    # API retry settings
    "api_max_retries": 3,
    "api_base_delay": 1.0,
    "api_max_delay": 60.0,

    # Database retry settings
    "db_max_retries": 5,
    "db_base_delay": 1.0,
    "db_max_delay": 30.0,
    "db_exponential_base": 2.0,

    # Runtime limits
    "max_runtime_minutes": 300,
    "default_batch_size": 10,

    # Quota settings
    "quota_limit": 10000,
    "quota_warn_threshold": 0.8,
    "quota_abort_threshold": 0.95,
    "quota_checkpoint_threshold": 500,

    # Performance tuning
    "default_comment_workers": 5,
    "progress_log_interval": 10,
    "progress_callback_interval": 5,
    "checkpoint_slow_threshold_ms": 100,

    # API constants
    "api_max_results_per_page": 50,
    "search_pagination_limit": 10,
}


```

The `Config` dataclass mirrors every key in `DEFAULTS`, adding type annotations. Secrets like `database_auth_token` and `postgres_url` are **excluded from DEFAULTS** — they must come from environment variables only.

```bash
sed -n '71,130p' scripts/config.py
```

```output
@dataclass
class Config:
    """
    Configuration container with typed access to all settings.

    Settings are loaded with priority: env vars > config file > defaults.
    Secrets (auth tokens) always come from environment variables.
    """

    # Database settings
    database_backend: str = DEFAULTS["database_backend"]
    database_url: str = DEFAULTS["database_url"]
    database_auth_token: str = ""  # Always from env var
    postgres_url: str = ""  # Always from env var when using postgres

    # Logging settings
    log_dir: str = DEFAULTS["log_dir"]
    log_level: str = DEFAULTS["log_level"]
    console_log_level: str = DEFAULTS["console_log_level"]

    # API retry settings
    api_max_retries: int = DEFAULTS["api_max_retries"]
    api_base_delay: float = DEFAULTS["api_base_delay"]
    api_max_delay: float = DEFAULTS["api_max_delay"]

    # Database retry settings
    db_max_retries: int = DEFAULTS["db_max_retries"]
    db_base_delay: float = DEFAULTS["db_base_delay"]
    db_max_delay: float = DEFAULTS["db_max_delay"]
    db_exponential_base: float = DEFAULTS["db_exponential_base"]

    # Runtime limits
    max_runtime_minutes: int = DEFAULTS["max_runtime_minutes"]
    default_batch_size: int = DEFAULTS["default_batch_size"]

    # Quota settings
    quota_limit: int = DEFAULTS["quota_limit"]
    quota_warn_threshold: float = DEFAULTS["quota_warn_threshold"]
    quota_abort_threshold: float = DEFAULTS["quota_abort_threshold"]
    quota_checkpoint_threshold: int = DEFAULTS["quota_checkpoint_threshold"]

    # Performance tuning
    default_comment_workers: int = DEFAULTS["default_comment_workers"]
    progress_log_interval: int = DEFAULTS["progress_log_interval"]
    progress_callback_interval: int = DEFAULTS["progress_callback_interval"]
    checkpoint_slow_threshold_ms: int = DEFAULTS["checkpoint_slow_threshold_ms"]

    # API constants
    api_max_results_per_page: int = DEFAULTS["api_max_results_per_page"]
    search_pagination_limit: int = DEFAULTS["search_pagination_limit"]

    # Source tracking (for debugging)
    _config_file: Optional[str] = None


# Global config instance (singleton pattern)
_config: Optional[Config] = None


def _load_yaml_settings(config_path: str) -> dict:
```

`load_config()` is where the three-level merge happens. It reads the YAML file's `settings:` block, then checks each environment variable, preferring it when set. The singleton `get_config()` caches the result so modules that call it multiple times pay the parsing cost only once.

```bash
sed -n '143,200p' scripts/config.py
```

```output
def _get_env_or_default(key: str, default, cast_type=None):
    """Get value from environment variable or return default."""
    env_value = os.environ.get(key)
    if env_value is None:
        return default
    if cast_type is not None:
        try:
            return cast_type(env_value)
        except (ValueError, TypeError):
            return default
    return env_value


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from file and environment variables.

    Args:
        config_path: Path to YAML config file (optional).
                    If not provided, tries default locations.

    Returns:
        Config object with all settings loaded.
    """
    # Try to find config file
    if config_path is None:
        # Check common locations
        candidates = [
            "config/channels.yaml",
            "../config/channels.yaml",
            "channels.yaml",
        ]
        for candidate in candidates:
            if Path(candidate).exists():
                config_path = candidate
                break

    # Load YAML settings
    yaml_settings = {}
    if config_path and Path(config_path).exists():
        yaml_settings = _load_yaml_settings(config_path)

    # Helper to get setting with priority: env > yaml > default
    def get_setting(yaml_key: str, env_key: str, default, cast_type=None):
        # First check environment variable (highest priority)
        env_value = os.environ.get(env_key)
        if env_value is not None:
            if cast_type is not None:
                try:
                    return cast_type(env_value)
                except (ValueError, TypeError):
                    pass
            return env_value
        # Then check YAML config file
        if yaml_key in yaml_settings:
            value = yaml_settings[yaml_key]
            if cast_type is not None:
                try:
```

Here is the actual channels.yaml — this is what operators edit to add/remove channels and tune behaviour per-channel or globally.

```bash
cat config/channels.yaml
```

```output
# YouTube Channels Configuration
# 
# List channels to track. Each channel can be specified by:
# - Channel ID (e.g., UC_x5XG1OV2P6uZZ5FSM9Ttw)
# - Handle (e.g., @GoogleDevelopers)
# - URL (e.g., https://www.youtube.com/@GoogleDevelopers)
#
# Per-channel options:
# - max_videos: Limit videos to fetch (most recent first)
# - max_video_age_days: Only fetch videos within this many days
# - fetch_comments: true/false (default: true)
# - max_comment_videos: Max videos to fetch comments for per run (default: 200)
# - max_comments_per_video: Max comments per video (default: 100)
# - min_new_comments: Skip videos with < N new comments (default: 10, set 0 to disable)
# - comment_workers: Parallel workers for comment fetching (default: 5)
# - video_discovery_mode: auto/search/playlist (default: auto)
# - comments_update_hours: Hours before re-fetching comments (default: 24)
# - comments_update_hours_new: Hours for videos < 7 days old (default: 6)
# - new_video_days: Videos younger than this are "new" (default: 7)
#
# NOTE: Transcripts are fetched separately using fetch_transcripts.py
# because YouTube blocks transcript requests from cloud/CI environments.
# Run locally: python fetch_transcripts.py --channel @YourChannel

channels:
  # Simple format - just the identifier
  - "@samwitteveenai"
  - "@aipapersacademy"
  - "@trychroma"
  - "@YannicKilcher"
  - "@code4AI"
  - "@GithubAwesome"
  - "@SebastianLague"
  - "@mojo_monday_gpu"
  - "@bashbunni"
  - "@NeuralNine"
  - "@AndyMath"
  - "@ChrisWilsonVideos"
  - "@MachineLearningStreetTalk"
  - "@AI-Makerspace"
  - "@googledeepmind"
  - "@anthropic-ai"
  - "@OpenAI"
  - "@podsaveamerica"
  - "@bulwarkmedia"
  - "@ivehaditpodcast"
  - "@MeidasTouch"
  
  
  # Detailed format with options
  # - identifier: "@mkbhd"
  #   max_videos: 500                # Only most recent 500 videos
  #   max_video_age_days: 365        # Only videos from last year
  #   fetch_comments: true
  #   max_comment_videos: 200        # Max videos to fetch comments for per run
  #   max_comments_per_video: 100    # Max comments per video
  #   min_new_comments: 10           # Skip videos with < 10 new comments
  #   comment_workers: 5             # Parallel workers for comments
  #   video_discovery_mode: auto     # auto, search, or playlist
  #   comments_update_hours: 24      # Re-check comments every 24h for older videos
  #   comments_update_hours_new: 4   # Re-check every 4h for new videos
  #   new_video_days: 7              # Videos < 7 days old are "new"
  
  # For high-engagement channels, limit comment scope to reduce runtime:
  # - identifier: "@podsaveamerica"
  #   max_comment_videos: 50         # Only fetch comments for 50 most recent videos
  #   max_comments_per_video: 50     # Cap at 50 comments per video
  #   min_new_comments: 20           # Skip videos with < 20 new comments
  #   comment_workers: 8             # Use more workers for faster processing

  # For very large/prolific channels, force playlist mode to detect deletions:
  # - identifier: "@TheMajorityReport"
  #   max_videos: 1000
  #   max_video_age_days: 90         # Only last 90 days
  #   video_discovery_mode: playlist # Always use playlist (detects deletions)
  #   fetch_comments: false          # Skip comments to save quota

# Global settings (can be overridden per-channel)
settings:
  # ============================================================================
  # DATABASE CONFIGURATION
  # ============================================================================
  # Database backend: "turso" (default) or "postgres"
  # NOTE: Turso/libsql backend auto-disables parallel workers due to thread-safety issues
  #       Use PostgreSQL backend for parallel processing
  database_backend: postgres

  # Database URL for Turso/libsql backend
  # Format: "file:path/to/local.db" for local SQLite or "libsql://your-db.turso.io" for remote
  # Priority: TURSO_DATABASE_URL env var > this setting > default
  database_url: file:data/youtube.db

  # NOTE: Authentication tokens should be set via environment variables for security:
  # - TURSO_AUTH_TOKEN: Auth token for remote Turso databases
  # - POSTGRES_URL: Full connection string for PostgreSQL (when database_backend: postgres)

  # ============================================================================
  # LOGGING CONFIGURATION
  # ============================================================================
  log_dir: logs
  log_level: DEBUG                   # File logging level
  console_log_level: INFO            # Console output level

  # ============================================================================
  # API RETRY SETTINGS
  # ============================================================================
  api_max_retries: 3                 # Max retries for YouTube API calls
  api_base_delay: 1.0                # Initial retry delay (seconds)
  api_max_delay: 60.0                # Maximum retry delay (seconds)

  # ============================================================================
  # DATABASE RETRY SETTINGS
  # ============================================================================
  db_max_retries: 5                  # Max retries for database operations
  db_base_delay: 1.0                 # Initial retry delay (seconds)
  db_max_delay: 30.0                 # Maximum retry delay (seconds)
  db_exponential_base: 2.0           # Exponential backoff multiplier

  # ============================================================================
  # RUNTIME LIMITS
  # ============================================================================
  max_runtime_minutes: 300           # Stop fetching after this (5 hours default)
  default_batch_size: 10             # Videos per batch for processing

  # ============================================================================
  # QUOTA SETTINGS
  # ============================================================================
  quota_limit: 10000                 # Daily API quota limit
  quota_warn_threshold: 0.8          # Warn at 80% usage
  quota_abort_threshold: 0.95        # Abort at 95% usage
  quota_checkpoint_threshold: 500    # Auto-save quota state every N units

  # ============================================================================
  # PERFORMANCE TUNING
  # ============================================================================
  default_comment_workers: 5         # Parallel workers for comment fetching
  progress_log_interval: 5          # Log progress every N batches
  progress_callback_interval: 5      # Progress callback frequency
  checkpoint_slow_threshold_ms: 100  # Warn if checkpoint serialization exceeds this

  # ============================================================================
  # API CONSTANTS
  # ============================================================================
  api_max_results_per_page: 50       # YouTube API max results per request
  search_pagination_limit: 10        # Max search API pages to prevent runaway

  # ============================================================================
  # CHANNEL FETCHING SETTINGS (can be overridden per-channel)
  # ============================================================================
  # Rate limiting: requests per second to YouTube API
  requests_per_second: 2.0

  # Comment fetching settings
  max_comments_per_video: 100        # Max comments per video (matches 1 API call)
  max_comment_videos: 200            # Max videos to fetch comments for per run
  min_new_comments: 10               # Skip videos with < N new comments (0 = disabled)
  comment_workers: 5                 # Parallel workers for comment fetching

  # Parallel workers for channel processing (1=sequential)
  # IMPORTANT: Keep at 1 - httplib2/SSL has thread-safety issues that cause
  # "double free or corruption" crashes when multiple threads hit SSL errors.
  # Comment workers within a single channel are still parallel and safe.
  channel_workers: 1

  # Video discovery mode:
  # - auto: Use search API for incremental updates, playlist for backfill (default)
  # - search: Always use search API (100 units/call, stops at known videos)
  # - playlist: Always use playlist API (1 unit/50 videos, fetches all, detects deletions)
  video_discovery_mode: auto

  # Comment update frequency
  comments_update_hours: 24        # Default for older videos
  comments_update_hours_new: 6     # Default for new videos (< new_video_days old)
  new_video_days: 7                # Videos younger than this get more frequent updates

  # Recommended constraints for large-scale tracking:
  # - Set max_video_age_days: 90-365 for ongoing analysis
  # - Set max_videos: 500-2000 for initial backfill
  # - For high-engagement channels: lower max_comment_videos and increase min_new_comments
  # - Use fetch_comments: false for channels with very high comment volume
```

## Section 3: Structured Logging — `scripts/logger.py`

The logging system has one key design goal: when many threads are fetching different channels in parallel, every log line should say **which channel** it came from. This is achieved with a Python thread-local variable that stores a short channel identifier string.

`ChannelContextFormatter` is a custom `logging.Formatter` subclass. On every call to `format()` it reads the thread-local context and prepends it to the message, e.g. `[samwit] Fetching 50 videos`.

`setup_logging()` creates two handlers:
- **Console handler** at INFO level (clean output in terminals/CI)
- **File handler** at DEBUG level (full detail in a timestamped log file)

A symlink `latest.log` always points to the most recent file.

```bash
sed -n '1,70p' scripts/logger.py
```

```output
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


```

```bash
sed -n '71,192p' scripts/logger.py
```

```output
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
```

## Section 4: Database Layer — `scripts/database.py`

`database.py` is the largest module (~2,500 lines) and acts as an abstraction layer over two very different databases: **Turso** (cloud SQLite via libSQL) and **PostgreSQL**. The rest of the codebase calls a single set of functions (`upsert_channel`, `insert_comments`, etc.) without caring which backend is active.

### Error handling & retry logic

Both backends can fail transiently (network blips, connection resets, Turso stream errors). The module defines three categories of error strings:

- `RETRYABLE_ERROR_PATTERNS` — safe to retry after a delay
- `CONNECTION_REFRESH_PATTERNS` — Turso stream errors; need a fresh connection object first
- `JWT_TOKEN_ERROR_PATTERNS` — auth token expired; fail immediately with a helpful message

The `@retry_db_operation` decorator wraps any function with exponential backoff up to `db_max_retries` attempts.

### TursoConnection

Turso's native libSQL driver has an internal streaming protocol. When the stream breaks mid-request it raises a specific error. `TursoConnection` wraps the raw libSQL connection and intercepts these errors with `_execute_with_refresh()`, transparently creating a new underlying connection before retrying.

```bash
sed -n '1,165p' scripts/database.py
```

```output
"""
Database schema and operations for YouTube metadata tracking.

Supports multiple backends:
- Turso (libSQL) - Cloud SQLite with edge replication
- PostgreSQL - Traditional database with excellent concurrency
- Local SQLite file (for development/testing)

Configuration can be set via config/channels.yaml settings section or environment variables:
- database_backend: "turso" (default) or "postgres"
- database_url: Connection URL for Turso/libsql
- TURSO_AUTH_TOKEN: Auth token (environment variable only, for security)
- POSTGRES_URL: PostgreSQL connection string (environment variable only)

Features:
- Automatic retry with exponential backoff for transient errors
- Connection wrapper for resilient database operations
- Thread-safe connection handling for parallel workers
"""

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional, Callable, Any

from config import get_config
from logger import get_logger

log = get_logger(__name__)


# ============================================================================
# RETRY LOGIC FOR TURSO DATABASE OPERATIONS
# ============================================================================

# Turso-specific error patterns that are retryable
RETRYABLE_ERROR_PATTERNS = [
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    "Connection reset",
    "Connection refused",
    "Connection timed out",
    "Temporary failure",
    "Too many requests",
    "SQLITE_BUSY",
    "database is locked",
    # Turso/Hrana stream errors - require connection refresh
    "stream not found",
    "Stream already in use",
]

# Patterns that indicate the connection needs to be recreated
CONNECTION_REFRESH_PATTERNS = [
    "stream not found",
    "Stream already in use",
]

# Patterns that indicate JWT token expiry or auth issues (not retryable)
JWT_TOKEN_ERROR_PATTERNS = [
    "token expired",
    "invalid JWT token",
    "jwt expired",
    "token has expired",
]

# Retry configuration helper functions - values loaded from config module
def get_db_max_retries() -> int:
    """Get max retries from config."""
    return get_config().db_max_retries

def get_db_base_delay() -> float:
    """Get base delay from config."""
    return get_config().db_base_delay

def get_db_max_delay() -> float:
    """Get max delay from config."""
    return get_config().db_max_delay

def get_db_exponential_base() -> float:
    """Get exponential base from config."""
    return get_config().db_exponential_base


def is_jwt_token_error(error: Exception) -> bool:
    """Check if an error is due to JWT token expiry or auth issues."""
    error_str = str(error).lower()
    for pattern in JWT_TOKEN_ERROR_PATTERNS:
        if pattern.lower() in error_str:
            return True
    return False


def is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable based on known patterns."""
    error_str = str(error).lower()
    for pattern in RETRYABLE_ERROR_PATTERNS:
        if pattern.lower() in error_str:
            return True
    return False


def needs_connection_refresh(error: Exception) -> bool:
    """Check if an error indicates the connection needs to be recreated."""
    error_str = str(error).lower()
    for pattern in CONNECTION_REFRESH_PATTERNS:
        if pattern.lower() in error_str:
            return True
    return False


def retry_db_operation(
    max_retries: int = None,
    base_delay: float = None,
    max_delay: float = None,
):
    """
    Decorator for retrying database operations with exponential backoff.

    Handles Turso-specific transient errors like 502, 503, connection issues.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get config values if not specified (allows runtime config)
            _max_retries = max_retries if max_retries is not None else get_db_max_retries()
            _base_delay = base_delay if base_delay is not None else get_db_base_delay()
            _max_delay = max_delay if max_delay is not None else get_db_max_delay()
            _exp_base = get_db_exponential_base()

            last_exception = None

            for attempt in range(_max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if is_retryable_error(e):
                        last_exception = e
                        if attempt < _max_retries:
                            delay = min(_base_delay * (_exp_base ** attempt), _max_delay)
                            log.warning(f"Database error (attempt {attempt + 1}/{_max_retries + 1}), "
                                       f"retrying in {delay:.1f}s: {e}")
                            time.sleep(delay)
                            continue
                        else:
                            log.error(f"Database operation failed after {_max_retries + 1} attempts: {e}")
                    else:
                        # Non-retryable error
                        log.error(f"Non-retryable database error: {e}")
                        raise

            # All retries exhausted
            raise last_exception

        return wrapper
    return decorator


class TursoConnection:
    """
    Wrapper around libsql connection with automatic retry logic.
```

Now let's look at the public API surface of `database.py` — all the functions that `fetch.py` and the other scripts call.

```bash
grep -n '^def \|^class ' scripts/database.py
```

```output
72:def get_db_max_retries() -> int:
76:def get_db_base_delay() -> float:
80:def get_db_max_delay() -> float:
84:def get_db_exponential_base() -> float:
89:def is_jwt_token_error(error: Exception) -> bool:
98:def is_retryable_error(error: Exception) -> bool:
107:def needs_connection_refresh(error: Exception) -> bool:
116:def retry_db_operation(
163:class TursoConnection:
284:class PostgresConnection:
509:def get_database_backend() -> str:
517:def is_postgres() -> bool:
522:def get_connection():
599:def init_database(conn) -> None:
810:def init_database_postgres(conn) -> None:
1013:def backfill_comment_summary(conn) -> int:
1075:def get_quota_usage(conn, date_str: str) -> Optional[dict]:
1099:def save_quota_usage(conn, date_str: str, used: int, operations: dict) -> None:
1125:def get_last_fetch_time(conn, channel_id: str) -> Optional[datetime]:
1138:def get_latest_video_publish_date(conn, channel_id: str) -> Optional[datetime]:
1151:def get_existing_video_ids(conn, channel_id: str) -> set[str]:
1159:def get_videos_without_transcripts(conn, channel_id: str) -> list[str]:
1170:def get_latest_comment_time(conn, video_id: str) -> Optional[datetime]:
1181:def get_videos_needing_comments(
1298:def get_videos_needing_stats_update(
1404:def _parse_datetime_utc(dt_string: str) -> datetime:
1424:def should_update_playlists(conn, channel_id: str, hours: int = 24) -> bool:
1438:def should_update_channel_stats(conn, channel_id: str, hours: int = 6) -> bool:
1456:def start_fetch_log(conn, channel_id: str, fetch_type: str) -> int:
1481:def complete_fetch_log(conn, fetch_id: int, 
1499:def upsert_channel(conn, channel: dict) -> None:
1538:def insert_channel_stats(conn, channel_id: str, stats: dict) -> None:
1554:def upsert_video(conn, video: dict, commit: bool = True) -> None:
1610:def insert_video_stats(conn, video_id: str, stats: dict, commit: bool = True) -> None:
1642:def insert_video_stats_batch(conn, video_stats: list[tuple[str, dict]]) -> int:
1695:def upsert_videos_batch(conn, videos: list[dict], commit: bool = True) -> int:
1768:def upsert_chapters_batch(conn, chapters_by_video: dict[str, list[dict]], commit: bool = True) -> int:
1809:def upsert_chapters(conn, video_id: str, chapters: list[dict], commit: bool = True) -> None:
1827:def insert_transcript(conn, video_id: str, transcript: dict) -> bool:
1859:def insert_comments(conn, comments: list[dict]) -> int:
1917:def upsert_playlist(conn, playlist: dict) -> None:
1953:def export_to_csv(conn, output_dir: str = "exports") -> dict[str, str]:
1992:def get_progress(conn, channel_id: str, fetch_id: int, operation: str) -> Optional[dict]:
2014:def get_checkpoint_slow_threshold_ms() -> int:
2019:def save_progress(conn, channel_id: str, fetch_id: int, operation: str,
2046:def clear_progress(conn, channel_id: str, fetch_id: int, operation: str = None) -> None:
2065:def get_all_video_ids_for_channel(conn, channel_id: str) -> set[str]:
2073:def mark_videos_as_deleted(conn, video_ids: list[str]) -> int:
2096:def get_deleted_videos(conn, channel_id: str = None) -> list[dict]:
2114:def purge_deleted_videos(conn, channel_id: str = None, older_than_days: int = 30) -> int:
```

The schema is created by `init_database()` (Turso/SQLite) or `init_database_postgres()`. Both create the same logical tables; the SQL syntax differs slightly. Here are all the tables:

```bash
grep -n 'CREATE TABLE' scripts/database.py
```

```output
610:        CREATE TABLE IF NOT EXISTS channels (
628:        CREATE TABLE IF NOT EXISTS channel_stats (
640:        CREATE TABLE IF NOT EXISTS videos (
670:        CREATE TABLE IF NOT EXISTS video_stats (
682:        CREATE TABLE IF NOT EXISTS chapters (
694:        CREATE TABLE IF NOT EXISTS transcripts (
707:        CREATE TABLE IF NOT EXISTS comments (
723:        CREATE TABLE IF NOT EXISTS playlists (
738:        CREATE TABLE IF NOT EXISTS fetch_log (
754:        CREATE TABLE IF NOT EXISTS fetch_progress (
767:        CREATE TABLE IF NOT EXISTS quota_usage (
795:        CREATE TABLE IF NOT EXISTS video_comment_summary (
817:        CREATE TABLE IF NOT EXISTS channels (
835:        CREATE TABLE IF NOT EXISTS channel_stats (
847:        CREATE TABLE IF NOT EXISTS videos (
877:        CREATE TABLE IF NOT EXISTS video_stats (
889:        CREATE TABLE IF NOT EXISTS chapters (
901:        CREATE TABLE IF NOT EXISTS transcripts (
914:        CREATE TABLE IF NOT EXISTS comments (
930:        CREATE TABLE IF NOT EXISTS playlists (
945:        CREATE TABLE IF NOT EXISTS fetch_log (
961:        CREATE TABLE IF NOT EXISTS fetch_progress (
974:        CREATE TABLE IF NOT EXISTS quota_usage (
998:        CREATE TABLE IF NOT EXISTS video_comment_summary (
```

```bash
sed -n '610,810p' scripts/database.py
```

```output
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            custom_url TEXT,
            country TEXT,
            published_at TEXT,
            thumbnail_url TEXT,
            banner_url TEXT,
            keywords TEXT,
            topic_categories TEXT,
            uploads_playlist_id TEXT,
            updated_at TEXT
        )
    """)
    
    # Channel stats time series
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channel_stats (
            channel_id TEXT,
            fetched_at TEXT,
            subscriber_count INTEGER,
            view_count INTEGER,
            video_count INTEGER,
            PRIMARY KEY (channel_id, fetched_at)
        )
    """)
    
    # Videos dimension table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            title TEXT,
            description TEXT,
            published_at TEXT,
            duration_seconds INTEGER,
            duration_iso TEXT,
            category_id TEXT,
            default_language TEXT,
            default_audio_language TEXT,
            tags TEXT,
            thumbnail_url TEXT,
            caption_available INTEGER,
            definition TEXT,
            dimension TEXT,
            projection TEXT,
            privacy_status TEXT,
            license TEXT,
            embeddable INTEGER,
            made_for_kids INTEGER,
            topic_categories TEXT,
            has_chapters INTEGER DEFAULT 0,
            first_seen_at TEXT,
            updated_at TEXT
        )
    """)
    
    # Video stats time series
    conn.execute("""
        CREATE TABLE IF NOT EXISTS video_stats (
            video_id TEXT,
            fetched_at TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            PRIMARY KEY (video_id, fetched_at)
        )
    """)
    
    # Chapters
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chapters (
            video_id TEXT,
            chapter_index INTEGER,
            title TEXT,
            start_seconds INTEGER,
            end_seconds INTEGER,
            PRIMARY KEY (video_id, chapter_index)
        )
    """)
    
    # Transcripts (write-once)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transcripts (
            video_id TEXT PRIMARY KEY,
            language TEXT,
            language_code TEXT,
            transcript_type TEXT,
            full_text TEXT,
            entries_json TEXT,
            fetched_at TEXT
        )
    """)
    
    # Comments
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            video_id TEXT,
            parent_comment_id TEXT,
            author_display_name TEXT,
            author_channel_id TEXT,
            text TEXT,
            like_count INTEGER,
            published_at TEXT,
            updated_at TEXT,
            fetched_at TEXT
        )
    """)
    
    # Playlists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id TEXT PRIMARY KEY,
            channel_id TEXT,
            title TEXT,
            description TEXT,
            published_at TEXT,
            thumbnail_url TEXT,
            item_count INTEGER,
            privacy_status TEXT,
            updated_at TEXT
        )
    """)
    
    # Fetch log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            fetch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT,
            fetch_type TEXT,
            started_at TEXT,
            completed_at TEXT,
            videos_fetched INTEGER,
            comments_fetched INTEGER,
            transcripts_fetched INTEGER,
            errors TEXT,
            status TEXT
        )
    """)
    
    # Fetch progress for resumable operations
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_progress (
            channel_id TEXT,
            fetch_id INTEGER,
            operation TEXT,
            processed_ids TEXT,
            total_count INTEGER,
            last_updated TEXT,
            PRIMARY KEY (channel_id, fetch_id, operation)
        )
    """)
    
    # Quota tracking (persists across runs)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quota_usage (
            date TEXT PRIMARY KEY,
            used INTEGER,
            operations TEXT,
            last_updated TEXT
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_fetched ON video_stats(fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_video ON video_stats(video_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_published ON comments(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_channel_stats_fetched ON channel_stats(fetched_at)")

    # Composite indexes for common query patterns
    # Optimizes queries like "get videos from channel X after date Y"
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_published ON videos(channel_id, published_at)")
    # Optimizes queries like "get comments on video X after date Y"
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_video_published ON comments(video_id, published_at)")
    # Optimizes finding latest stats for a video (used in ROW_NUMBER queries)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_video_fetched ON video_stats(video_id, fetched_at DESC)")

    # Summary table for fast comment/stats lookups
    # Eliminates expensive COUNT(*) and MAX() aggregations on comments table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS video_comment_summary (
            video_id TEXT PRIMARY KEY,
            stored_comment_count INTEGER DEFAULT 0,
            last_comment_fetch TEXT,
            latest_youtube_comment_count INTEGER,
            last_stats_fetch TEXT
        )
    """)

    conn.commit()

    # Backfill summary table with existing data (no-op if already populated)
    backfill_comment_summary(conn)


def init_database_postgres(conn) -> None:
```

## Section 5: YouTube API Client — `scripts/youtube_api.py`

`youtube_api.py` wraps Google's `google-api-python-client` library and adds production-grade resilience:

- **`@retry_with_backoff` decorator** — retries any decorated method on HTTP 429/5xx responses or transient network errors, with exponential backoff up to `api_max_delay`
- **Rate limiter** (`_rate_limit()`) — ensures calls are spaced at least `1/requests_per_second` seconds apart
- **`resolve_channel_id()`** — converts a YouTube handle (`@samwitteveenai`) or URL to the internal `UC...` channel ID format, needed for all other API calls
- **Thread-safety** — each parallel worker thread creates its own `YouTubeFetcher` instance (and therefore its own SSL/HTTP session), avoiding the thread-safety issues of sharing a single client

First, the retry decorator:

```bash
sed -n '33,153p' scripts/youtube_api.py
```

```output
# ============================================================================
# CONSTANTS & CONFIG HELPERS
# ============================================================================

# Retry status codes (fixed, not configurable)
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# Config helper functions
def get_api_max_results_per_page() -> int:
    """Get max results per page from config."""
    return get_config().api_max_results_per_page

def get_search_pagination_limit() -> int:
    """Get search pagination limit from config."""
    return get_config().search_pagination_limit

def get_progress_log_interval() -> int:
    """Get progress log interval from config."""
    return get_config().progress_log_interval

def get_api_max_retries() -> int:
    """Get API max retries from config."""
    return get_config().api_max_retries

def get_api_base_delay() -> float:
    """Get API base delay from config."""
    return get_config().api_base_delay

def get_api_max_delay() -> float:
    """Get API max delay from config."""
    return get_config().api_max_delay


def retry_with_backoff(
    max_retries: int = None,
    base_delay: float = None,
    max_delay: float = None,
    exponential_base: float = 2.0,
):
    """
    Decorator for retrying functions with exponential backoff.

    Retries on:
    - HTTP 429 (rate limit)
    - HTTP 5xx (server errors)
    - Connection errors
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get config values if not specified (allows runtime config)
            _max_retries = max_retries if max_retries is not None else get_api_max_retries()
            _base_delay = base_delay if base_delay is not None else get_api_base_delay()
            _max_delay = max_delay if max_delay is not None else get_api_max_delay()

            last_exception = None

            for attempt in range(_max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except HttpError as e:
                    status_code = e.resp.status if hasattr(e, 'resp') else None

                    if status_code in RETRYABLE_STATUS_CODES:
                        last_exception = e
                        if attempt < _max_retries:
                            delay = min(_base_delay * (exponential_base ** attempt), _max_delay)

                            # Check for Retry-After header
                            retry_after = e.resp.get('retry-after') if hasattr(e, 'resp') else None
                            if retry_after:
                                try:
                                    delay = max(delay, float(retry_after))
                                except ValueError:
                                    pass

                            log.warning(f"HTTP {status_code} error, retrying in {delay:.1f}s "
                                       f"(attempt {attempt + 1}/{_max_retries + 1}): {e}")
                            time.sleep(delay)
                            continue
                    else:
                        # Non-retryable HTTP error
                        log.error(f"Non-retryable HTTP error {status_code}: {e}")
                        raise

                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        ConnectionResetError,
                        TimeoutError,
                        ssl.SSLError,
                        OSError,
                        AttributeError) as e:
                    # OSError catches low-level network errors including SSLEOFError
                    # AttributeError catches httplib2 thread-safety issues ('NoneType' has no attribute 'read')
                    last_exception = e
                    if attempt < _max_retries:
                        delay = min(_base_delay * (exponential_base ** attempt), _max_delay)
                        log.warning(f"Connection error, retrying in {delay:.1f}s "
                                   f"(attempt {attempt + 1}/{_max_retries + 1}): {type(e).__name__}: {e}")
                        time.sleep(delay)
                        continue

                except Exception as e:
                    # Non-retryable exception
                    log.error(f"Non-retryable error in {func.__name__}: {type(e).__name__}: {e}")
                    raise

            # All retries exhausted
            log.error(f"All {_max_retries + 1} attempts failed for {func.__name__}")
            raise last_exception

        return wrapper
    return decorator


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_video_data(video: dict) -> dict:
```

```bash
sed -n '201,270p' scripts/youtube_api.py
```

```output
class YouTubeFetcher:
    """Handles fetching data from YouTube API with rate limiting and retry logic."""
    
    def __init__(self, api_key: str = None, requests_per_second: float = 2.0):
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY not provided")
        
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.min_request_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        
        log.debug(f"YouTubeFetcher initialized, rate limit: {requests_per_second} req/s")
    
    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            log.debug(f"Rate limiting: sleeping {sleep_time:.3f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    @retry_with_backoff()
    def resolve_channel_id(self, identifier: str) -> str:
        """Resolve various channel identifiers to a channel ID."""
        log.debug(f"Resolving channel identifier: {identifier}")
        identifier = identifier.strip()
        
        # Direct channel ID
        if identifier.startswith("UC") and len(identifier) == 24:
            log.debug(f"Direct channel ID: {identifier}")
            return identifier
        
        # Handle (@username)
        if identifier.startswith("@"):
            handle = identifier.lstrip("@")
        elif "youtube.com/@" in identifier:
            match = re.search(r"youtube\.com/@([\w.-]+)", identifier)
            handle = match.group(1) if match else None
        elif "youtube.com/channel/" in identifier:
            match = re.search(r"youtube\.com/channel/(UC[\w-]{22})", identifier)
            if match:
                log.debug(f"Extracted channel ID from URL: {match.group(1)}")
                return match.group(1)
            handle = None
        else:
            # Assume it's a handle without @
            handle = identifier
        
        if handle:
            log.debug(f"Looking up handle: {handle}")
            self._rate_limit()
            request = self.youtube.channels().list(
                part="id",
                forHandle=handle
            )
            response = request.execute()
            
            if response.get("items"):
                channel_id = response["items"][0]["id"]
                log.debug(f"Resolved handle '{handle}' to channel ID: {channel_id}")
                return channel_id
        
        raise ValueError(f"Could not resolve channel ID for: {identifier}")
    
    @retry_with_backoff()
    def fetch_channel(self, channel_id: str) -> dict:
        """Fetch comprehensive channel metadata."""
        log.debug(f"Fetching channel metadata for: {channel_id}")
```

Here is an overview of all the fetch methods on `YouTubeFetcher`:

```bash
grep -n 'def fetch_\|def _fetch_\|def resolve_\|def search_' scripts/youtube_api.py
```

```output
225:    def resolve_channel_id(self, identifier: str) -> str:
268:    def fetch_channel(self, channel_id: str) -> dict:
313:    def _fetch_playlist_page(self, playlist_id: str, page_token: str = None) -> dict:
324:    def fetch_playlist_video_ids(self, playlist_id: str, max_results: int = None) -> list[str]:
383:    def search_channel_videos(
452:    def _fetch_videos_batch(self, video_ids: list[str]) -> list[dict]:
461:    def fetch_videos(self, video_ids: list[str]) -> list[dict]:
615:    def _fetch_comments_page(self, video_id: str, max_results: int, page_token: str = None) -> dict:
628:    def fetch_comments(
719:    def _fetch_playlists_page(self, channel_id: str, page_token: str = None) -> dict:
730:    def fetch_playlists(self, channel_id: str) -> list[dict]:
```

## Section 6: Quota Tracking — `scripts/quota.py`

The YouTube Data API v3 gives you exactly **10,000 units per day**. Each operation costs a different amount:

| Operation | Cost |
|-----------|------|
| `search.list` | 100 units |
| `channels.list` | 1 unit |
| `videos.list` | 1 unit |
| `commentThreads.list` | 1 unit |
| `comments.list` (replies) | 1 unit |
| `playlistItems.list` | 1 unit |

`QuotaTracker` wraps a running counter that is **persisted to the database** after every `quota_checkpoint_threshold` units (default: 500). This means quota state survives crashes and process restarts — a long run won't accidentally over-count because it lost the counter in memory.

Key methods:
- `use(operation, count)` — deducts units and triggers auto-checkpoint
- `_check_thresholds()` — warns at 80%, raises `QuotaExhaustedError` at 95%
- `can_afford(operation)` — used before expensive search calls to bail early

```bash
sed -n '26,110p' scripts/quota.py
```

```output
class QuotaExhaustedError(Exception):
    """Raised when API quota is exhausted or insufficient for operation."""
    pass


class QuotaTracker:
    """
    Track YouTube API quota usage across runs.

    Persists quota usage to database so we can track across multiple runs in a day.
    Uses a dedicated database connection to avoid conflicts with worker threads.

    Features:
    - Thread-safe quota tracking with minimal lock contention
    - Auto-checkpoint every N quota units for crash safety
    - Explicit flush() for phase transitions
    """

    # API operation costs (units)
    # See: https://developers.google.com/youtube/v3/determine_quota_cost
    COSTS = {
        'channels.list': 1,
        'playlists.list': 1,
        'playlistItems.list': 1,
        'videos.list': 1,
        'commentThreads.list': 1,
        'comments.list': 1,
        'search.list': 100,  # Very expensive - avoid!
        'captions.list': 50,
    }

    def __init__(
        self,
        daily_limit: int = None,
        warn_threshold: float = None,
        abort_threshold: float = None,
        checkpoint_threshold: int = None,
    ):
        """
        Initialize quota tracker with dedicated database connection.

        Args:
            daily_limit: Daily quota limit (default: from config or 10000)
            warn_threshold: Fraction of quota at which to warn (default: from config or 0.8)
            abort_threshold: Fraction of quota at which to abort (default: from config or 0.95)
            checkpoint_threshold: Auto-save quota state every N units spent (default: from config or 500)
        """
        cfg = get_config()
        self.daily_limit = daily_limit if daily_limit is not None else cfg.quota_limit
        self.warn_threshold = warn_threshold if warn_threshold is not None else cfg.quota_warn_threshold
        self.abort_threshold = abort_threshold if abort_threshold is not None else cfg.quota_abort_threshold
        self._checkpoint_threshold = checkpoint_threshold if checkpoint_threshold is not None else cfg.quota_checkpoint_threshold

        self.today = date.today().isoformat()
        self.used = 0
        self.operations = {}  # Track by operation type
        self.session_used = 0  # Just this run
        self.session_start = datetime.now()

        # Thread-safety: separate locks for quota state and DB operations
        self._lock = threading.Lock()  # Protects quota state (fast operations)
        self._db_lock = threading.Lock()  # Serializes DB saves (slow operations)

        # Checkpoint tracking
        self._dirty = False  # Has unsaved changes
        self._since_checkpoint = 0  # Quota units since last save

        # Load initial state (creates temporary connection)
        self._load_state()

        log.info(f"Quota tracker initialized: limit={self.daily_limit}, used_today={self.used}")
        if self.used > 0:
            log.info(f"Resumed from previous runs: {self.used} units already used today")
        log.debug(f"Thresholds: warn={self.warn_threshold}, abort={self.abort_threshold}, "
                  f"checkpoint={self._checkpoint_threshold}")

    def _get_connection(self):
        """
        Create a fresh database connection for quota operations.

        Uses the database module's get_connection() which respects the
        configured backend (PostgreSQL or Turso/libsql).
        """
        return get_db_connection()

```

```bash
sed -n '167,260p' scripts/quota.py
```

```output
    def use(self, operation: str, count: int = 1) -> int:
        """
        Record quota usage for an operation. Thread-safe with auto-checkpoint.

        Fast path: just update counters and mark dirty.
        Auto-checkpoint triggers save when threshold reached.

        Args:
            operation: API operation name (e.g., 'videos.list')
            count: Number of API calls made

        Returns:
            Cost in quota units
        """
        cost = self.COSTS.get(operation, 1) * count
        should_checkpoint = False

        with self._lock:
            self.used += cost
            self.session_used += cost
            self.operations[operation] = self.operations.get(operation, 0) + cost
            self._since_checkpoint += cost
            self._dirty = True
            current_used = self.used

            # Check if we should auto-checkpoint
            if self._since_checkpoint >= self._checkpoint_threshold:
                should_checkpoint = True

        log.debug(f"Quota: +{cost} for {operation} x{count} (total: {current_used}/{self.daily_limit})")

        # Auto-checkpoint for large channels (outside quota lock to reduce contention)
        if should_checkpoint:
            self.flush()

        self._check_thresholds()

        return cost

    def flush(self):
        """
        Explicitly save quota state to database.

        Call this at phase transitions (after videos, after comments, etc.)
        or at channel completion. Thread-safe.
        """
        with self._db_lock:
            with self._lock:
                if not self._dirty:
                    return  # Nothing to save
                self._dirty = False
                self._since_checkpoint = 0

            self._save_state()
    
    def _check_thresholds(self):
        """Check if we've hit warning or abort thresholds."""
        usage_fraction = self.used / self.daily_limit
        
        if usage_fraction >= self.abort_threshold:
            log.error(f"QUOTA CRITICAL: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
        elif usage_fraction >= self.warn_threshold:
            log.warning(f"QUOTA WARNING: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
    
    def remaining(self) -> int:
        """Get remaining quota units. Thread-safe."""
        with self._lock:
            return max(0, self.daily_limit - self.used)

    def used_fraction(self) -> float:
        """Get fraction of quota used. Thread-safe."""
        with self._lock:
            return self.used / self.daily_limit

    def can_afford(self, operation: str, count: int = 1) -> bool:
        """Check if we can afford an operation without exceeding abort threshold. Thread-safe."""
        cost = self.COSTS.get(operation, 1) * count
        with self._lock:
            projected = self.used + cost
        return projected <= (self.daily_limit * self.abort_threshold)
    
    def estimate_channel_cost(
        self, 
        video_count: int, 
        fetch_comments: bool = True,
        fetch_transcripts: bool = True,
        max_comments_per_video: int = 100
    ) -> dict:
        """
        Estimate quota cost to fully fetch a channel.
        
        Returns dict with breakdown by operation.
        """
        estimates = {
```

## Section 7: The Main Fetch Pipeline — `scripts/fetch.py`

`fetch.py` is the orchestration layer that ties everything together. It is the script that runs in GitHub Actions and that you invoke manually. The overall flow for each channel is four sequential phases:

    Phase 1 — Channel metadata   (channels.list, 1 unit)
    Phase 2 — Video discovery    (search.list 100 units/page  OR  playlistItems.list 1 unit/50)
    Phase 3 — Video stats        (videos.list, 1 unit per batch of up to 50)
    Phase 4 — Comments           (commentThreads.list + comments.list, parallel workers)

### Smart incremental updates

The script skips work it already did recently:
- Channel stats are skipped if fetched within the last 6 hours
- Incremental video discovery stops as soon as it sees a video already in the database
- Comments for old videos are only re-fetched every 24 hours; new videos every 6 hours

### Checkpointing

After every batch of videos or comments the current progress is serialised to `fetch_progress` in the database. If the run is killed mid-way the next invocation resumes from the last checkpoint rather than starting over.

Let's look at the imports and module initialisation:

```bash
sed -n '1,80p' scripts/fetch.py
```

```output
#!/usr/bin/env python3
"""
YouTube Channel Metadata Fetcher

Fetches and stores YouTube channel metadata in Turso/SQLite for ongoing analysis.
Features:
- Smart incremental updates to minimize redundant API requests
- Quota tracking to prevent exceeding daily limits
- Checkpointing for resumable operations
- Parallel comment fetching for faster processing
- Detailed DEBUG logging for development

Usage:
    python fetch.py --channel @GoogleDevelopers
    python fetch.py --config config/channels.yaml
    python fetch.py --channel @GoogleDevelopers --backfill
    python fetch.py --export
"""

import argparse
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import yaml

# Initialize config first so logging can use it
from config import get_config

# Initialize logging with config
from logger import setup_logging, get_logger, LogContext, set_channel_context, clear_channel_context

# Set up logging before other imports (will use config values)
logger = setup_logging()
log = get_logger("fetch")

from database import (
    get_connection,
    init_database,
    is_postgres,
    get_last_fetch_time,
    get_latest_video_publish_date,
    get_existing_video_ids,
    get_videos_needing_stats_update,
    get_videos_without_transcripts,
    get_latest_comment_time,
    get_videos_needing_comments,
    should_update_channel_stats,
    should_update_playlists,
    start_fetch_log,
    complete_fetch_log,
    upsert_channel,
    insert_channel_stats,
    upsert_video,
    upsert_videos_batch,
    insert_video_stats,
    insert_video_stats_batch,
    upsert_chapters,
    upsert_chapters_batch,
    insert_transcript,
    insert_comments,
    upsert_playlist,
    export_to_csv,
    get_progress,
    save_progress,
    clear_progress,
    get_all_video_ids_for_channel,
    mark_videos_as_deleted,
)
from googleapiclient.errors import HttpError
from youtube_api import YouTubeFetcher
from quota import QuotaTracker, QuotaExhaustedError


# Configuration helper functions (values loaded from config module)
```

Now let's see the full function landscape of `fetch.py`, and then look at the parallel comment-fetching function which is the most complex part:

```bash
grep -n '^def \|^class ' scripts/fetch.py
```

```output
81:def get_default_batch_size() -> int:
85:def get_max_runtime_minutes() -> int:
89:def get_default_comment_workers() -> int:
93:def get_progress_log_interval() -> int:
97:def get_progress_callback_interval() -> int:
102:def load_config(config_path: str) -> dict:
111:def fetch_comments_parallel(
257:def fetch_channel_data(
856:def _video_is_recent(video: dict, cutoff_date: datetime) -> bool:
881:def dry_run_channel(
966:def main():
```

```bash
sed -n '111,260p' scripts/fetch.py
```

```output
def fetch_comments_parallel(
    api_key: str,
    conn,
    quota: QuotaTracker,
    video_ids: list[str],
    max_comments_per_video: int,
    backfill: bool,
    num_workers: int = None,
    max_replies_per_comment: int = 10,
    progress_callback: Optional[Callable[[int, int, int], None]] = None,
) -> tuple[int, int, list[str]]:
    """
    Fetch comments for multiple videos in parallel.

    Creates a separate API client per thread to avoid SSL/connection issues.

    Args:
        api_key: YouTube API key (each thread creates its own client)
        conn: Database connection
        quota: QuotaTracker instance
        video_ids: List of video IDs to fetch comments for
        max_comments_per_video: Max comments per video
        backfill: If True, fetch all comments; otherwise only new ones
        num_workers: Number of parallel workers (default: 3)
        max_replies_per_comment: Max replies per top-level comment (default: 10)
        progress_callback: Optional callback(processed, total, new_comments)

    Returns:
        Tuple of (total_comments, new_comments, errors)
    """
    # Use config defaults if not specified
    if num_workers is None:
        num_workers = get_default_comment_workers()

    if not video_ids:
        return 0, 0, []

    # Thread-safe counters
    lock = threading.Lock()
    total_comments = 0
    new_comments = 0
    errors = []
    processed = 0
    stop_flag = threading.Event()
    
    # Thread-local storage for per-thread API clients
    thread_local = threading.local()
    
    def get_thread_fetcher():
        """Get or create a YouTubeFetcher for the current thread."""
        if not hasattr(thread_local, 'fetcher'):
            thread_local.fetcher = YouTubeFetcher(api_key=api_key)
        return thread_local.fetcher
    
    # Pre-fetch 'since' times for incremental mode (avoid DB access in threads)
    since_times = {}
    if not backfill:
        for video_id in video_ids:
            since_times[video_id] = get_latest_comment_time(conn, video_id)
    
    def fetch_single_video(video_id: str) -> tuple[str, list, int, str | None]:
        """Fetch comments for a single video. Returns (video_id, comments, quota_used, error)."""
        if stop_flag.is_set():
            return video_id, [], 0, "stopped"

        # Get thread-local fetcher
        fetcher = get_thread_fetcher()
        since = since_times.get(video_id) if not backfill else None

        try:
            comments = fetcher.fetch_comments(
                video_id,
                since=since,
                max_results=max_comments_per_video,
                max_replies_per_comment=max_replies_per_comment
            )
            quota_used = (len(comments) // 100) + 1 if comments else 1
            return video_id, comments, quota_used, None
        except HttpError as e:
            # Handle YouTube API errors specifically
            error_msg = str(e)
            if "commentsDisabled" in error_msg or e.resp.status == 403:
                log.debug(f"Comments disabled for {video_id}")
            else:
                log.warning(f"HTTP error fetching comments for {video_id}: {e}")
            return video_id, [], 1, error_msg
        except (ConnectionError, TimeoutError, OSError) as e:
            # Handle network-related errors
            log.warning(f"Network error fetching comments for {video_id}: {e}")
            return video_id, [], 1, str(e)
        except Exception as e:
            # Catch-all for unexpected errors in thread context
            log.warning(f"Unexpected error fetching comments for {video_id}: {type(e).__name__}: {e}")
            return video_id, [], 1, str(e)
    
    # Process with thread pool
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(fetch_single_video, vid): vid for vid in video_ids}
        
        for future in as_completed(futures):
            video_id = futures[future]
            
            # Check quota before processing result
            with lock:
                if not quota.can_afford('commentThreads.list', 1):
                    log.warning("Insufficient quota, stopping comment fetch")
                    stop_flag.set()
                    break
            
            try:
                vid, comments, quota_used, error = future.result()

                with lock:
                    # Update quota
                    quota.use('commentThreads.list', quota_used)

                    if error and "stopped" not in error:
                        errors.append(f"Comments {vid}: {error}")

                    if comments:
                        # Database write - SQLite handles this safely with WAL mode
                        new_count = insert_comments(conn, comments)
                        total_comments += len(comments)
                        new_comments += new_count
                        log.debug(f"Video {vid}: {len(comments)} comments ({new_count} new)")

                    processed += 1

                    # Progress callback
                    if progress_callback and processed % get_progress_callback_interval() == 0:
                        progress_callback(processed, len(video_ids), new_comments)

            except (KeyboardInterrupt, SystemExit):
                # Re-raise system-level interrupts
                raise
            except Exception as e:
                # Handle any other errors from future execution
                with lock:
                    log.debug(f"Future result error for {video_id}: {type(e).__name__}: {e}")
                    errors.append(f"Comments {video_id}: {str(e)}")
                    processed += 1
    
    return total_comments, new_comments, errors


def fetch_channel_data(
    fetcher: YouTubeFetcher,
    conn,
    quota: QuotaTracker,
```

The key pattern in `fetch_comments_parallel`: each worker thread calls `get_thread_fetcher()` which lazily creates a `YouTubeFetcher` bound to that thread's `threading.local()` storage. This ensures no two threads share an HTTP connection or SSL context.

Now let's look at where parallelism and checkpointing happen throughout the file:

```bash
grep -n 'ThreadPoolExecutor\|as_completed\|quota\.use\|quota\.flush\|save_progress\|get_progress\|clear_progress' scripts/fetch.py
```

```output
25:from concurrent.futures import ThreadPoolExecutor, as_completed
69:    get_progress,
70:    save_progress,
71:    clear_progress,
93:def get_progress_log_interval() -> int:
97:def get_progress_callback_interval() -> int:
207:    with ThreadPoolExecutor(max_workers=num_workers) as executor:
211:        for future in as_completed(futures):
226:                    quota.use('commentThreads.list', quota_used)
241:                    if progress_callback and processed % get_progress_callback_interval() == 0:
361:        quota.use('channels.list')  # Resolution uses API
391:            quota.use('channels.list')
485:            quota.flush()
495:                quota.use('playlists.list', (len(playlists) // 50) + 1)
540:                quota.use('search.list', search_api_calls)  # 100 units per search call (cost is in COSTS dict)
555:                quota.use('playlistItems.list', (len(all_video_ids) // 50) + 1)
585:        progress = get_progress(conn, channel_id, fetch_id, 'videos')
619:                        quota.use('videos.list', 1)
649:                        save_progress(conn, channel_id, fetch_id, 'videos',
653:                    if batch_num % get_progress_log_interval() == 0 or batch_num == num_batches:
669:            quota.flush()
703:                            quota.use('videos.list', 1)
710:                            if batch_num % get_progress_log_interval() == 0 or batch_num == total_batches:
726:                    quota.flush()
761:                comment_progress = get_progress(conn, channel_id, fetch_id, 'comments')
793:                    quota.flush()
802:        clear_progress(conn, channel_id, fetch_id)
819:        quota.flush()  # Final checkpoint on success
823:        quota.flush()  # Save quota state on error
833:        quota.flush()  # Save quota state on timeout
842:        quota.flush()  # Save quota state on failure
901:    quota.use('channels.list')
905:    quota.use('channels.list')
1126:    if quota.used >= quota.daily_limit:
1127:        log.error(f"Quota already exhausted: {quota.used}/{quota.daily_limit}")
1322:        with ThreadPoolExecutor(max_workers=channel_workers) as executor:
1325:            for future in as_completed(futures):
```

Finally, the `main()` function — this is the entry point when `fetch.py` is run. It parses CLI arguments, initialises the database and quota tracker, then processes channels either sequentially or in parallel:

```bash
sed -n '966,1100p' scripts/fetch.py
```

```output
def main():
    parser = argparse.ArgumentParser(
        description="Fetch YouTube channel metadata into Turso/SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--channel", "-c",
        help="Single channel ID, handle (@username), or URL"
    )
    input_group.add_argument(
        "--config",
        help="Path to channels config YAML file"
    )
    input_group.add_argument(
        "--export",
        action="store_true",
        help="Export database to CSV files"
    )
    
    # Fetch options
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Full backfill mode (ignore incremental, fetch everything)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be fetched without actually fetching"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        help="Maximum videos to fetch per channel (most recent first)"
    )
    parser.add_argument(
        "--max-video-age",
        type=int,
        help="Only fetch videos published within this many days"
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=100,
        help="Maximum comments per video (default: 100)"
    )
    parser.add_argument(
        "--max-replies",
        type=int,
        default=10,
        help="Maximum replies per top-level comment (default: 10, prevents blowup on viral comments)"
    )
    parser.add_argument(
        "--max-comment-videos",
        type=int,
        default=200,
        help="Maximum videos to fetch comments for per run (default: 200, prevents extremely long runs)"
    )
    parser.add_argument(
        "--min-new-comments",
        type=int,
        default=10,
        help="Skip videos with fewer than this many new comments since last fetch (default: 10, set to 0 to disable)"
    )
    parser.add_argument(
        "--video-discovery-mode",
        type=str,
        choices=["auto", "search", "playlist"],
        default="auto",
        help="Video discovery strategy: auto (search for incremental, playlist for backfill), "
             "search (100 units/call, stops at known), playlist (1 unit/50 videos, fetches all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Videos to process per batch/commit (default: from config)"
    )
    parser.add_argument(
        "--stats-update-hours",
        type=int,
        default=6,
        help="Only update channel stats if older than this (default: 6 hours)"
    )
    parser.add_argument(
        "--comment-workers",
        type=int,
        default=None,
        help="Parallel workers for comment fetching (default: from config)"
    )
    parser.add_argument(
        "--channel-workers",
        type=int,
        default=None,
        help="Parallel workers for channel processing (default: from config, 1=sequential)"
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=None,
        help="Stop fetching after this many minutes (default: from config)"
    )
    parser.add_argument(
        "--skip-comments",
        action="store_true",
        help="Skip fetching comments"
    )
    parser.add_argument(
        "--quota-limit",
        type=int,
        default=None,
        help="Daily API quota limit (default: from config)"
    )
    parser.add_argument(
        "--reset-quota",
        action="store_true",
        help="Reset today's quota counter to 0 (use if quota tracking was corrupted)"
    )
    
    args = parser.parse_args()
    
    # Resolve config defaults for arguments that weren't specified
    cfg = get_config()
    if args.batch_size is None:
        args.batch_size = cfg.default_batch_size
    if args.comment_workers is None:
        args.comment_workers = cfg.default_comment_workers
    if args.channel_workers is None:
        args.channel_workers = 1  # Default from config settings section
    if args.max_runtime_minutes is None:
        args.max_runtime_minutes = cfg.max_runtime_minutes
    if args.quota_limit is None:
```

```bash
tail -140 scripts/fetch.py
```

```output
            channel_options = channel_config

        # Get global settings from config, merge with channel-specific options
        global_settings = config.get("settings", {})

        # Channel options override global settings
        def get_option(key, default):
            return channel_options.get(key, global_settings.get(key, default))

        # For parallel processing, create dedicated fetcher and connection per thread
        if args.channel_workers > 1:
            thread_fetcher = YouTubeFetcher(fetcher.api_key)
            thread_conn = get_connection()
        else:
            thread_fetcher = fetcher
            thread_conn = conn

        try:
            stats = fetch_channel_data(
                fetcher=thread_fetcher,
                conn=thread_conn,
                quota=quota,
                channel_identifier=identifier,
                fetch_comments=not args.skip_comments and channel_options.get("fetch_comments", True),
                max_videos=args.max_videos or channel_options.get("max_videos"),
                max_video_age_days=args.max_video_age or channel_options.get("max_video_age_days"),
                max_comments_per_video=get_option("max_comments_per_video", args.max_comments),
                max_replies_per_comment=get_option("max_replies_per_comment", args.max_replies),
                max_comment_videos=get_option("max_comment_videos", args.max_comment_videos),
                min_new_comments=get_option("min_new_comments", args.min_new_comments),
                comment_workers=get_option("comment_workers", args.comment_workers),
                video_discovery_mode=get_option("video_discovery_mode", args.video_discovery_mode),
                batch_size=args.batch_size,
                stats_update_hours=get_option("stats_update_hours", args.stats_update_hours),
                backfill=args.backfill,
                start_time=start_time,
                max_runtime_minutes=args.max_runtime_minutes,
            )

            with stats_lock:
                if stats.get("skipped"):
                    total_stats["channels_skipped"] += 1
                else:
                    total_stats["channels"] += 1

                total_stats["videos"] += stats["videos_fetched"]
                total_stats["videos_new"] += stats["videos_new"]
                total_stats["videos_stats_updated"] += stats.get("videos_stats_updated", 0)
                total_stats["comments"] += stats["comments_fetched"]
                total_stats["transcripts"] += stats.get("transcripts_fetched", 0)
                total_stats["errors"] += len(stats["errors"])

            return "success"

        except QuotaExhaustedError:
            log.error(f"Quota exhausted while processing {identifier}")
            stop_flag.set()
            with stats_lock:
                total_stats["channels_skipped"] += 1
            return "quota_exhausted"

        except Exception as e:
            log.exception(f"Error processing {identifier}: {e}")
            with stats_lock:
                total_stats["errors"] += 1
            return "error"

    # Process channels - parallel or sequential
    # Channel workers can be set via CLI or config file (CLI takes precedence)
    global_settings = config.get("settings", {})
    channel_workers = args.channel_workers if args.channel_workers != 1 else global_settings.get("channel_workers", 1)

    # IMPORTANT: Parallel channel workers are disabled due to thread-safety issues
    # in httplib2/SSL (used by google-api-python-client). When multiple threads
    # encounter SSL errors simultaneously, it causes "double free or corruption" crashes.
    # This affects both Turso and PostgreSQL backends.
    # Note: Comment workers within a single channel are still parallel and safe.
    if channel_workers > 1:
        log.warning("Parallel channel workers disabled due to httplib2/SSL thread-safety issues")
        log.warning("Processing channels sequentially. Comment fetching remains parallel.")
        channel_workers = 1

    if channel_workers > 1:
        log.info(f"Processing {len(channels)} channels with {channel_workers} parallel workers")
        with ThreadPoolExecutor(max_workers=channel_workers) as executor:
            futures = {executor.submit(process_single_channel, ch): ch for ch in channels}

            for future in as_completed(futures):
                result = future.result()
                if result == "timeout":
                    log.warning(f"Approaching time limit, stopping new channel processing")
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break
                elif result == "quota_exhausted":
                    log.error("Quota exhausted, stopping all channel processing")
                    for f in futures:
                        f.cancel()
                    break
    else:
        # Sequential processing (original behavior)
        for channel_config in channels:
            result = process_single_channel(channel_config)
            if result == "timeout":
                log.warning(f"Approaching time limit, stopping")
                break
            elif result == "quota_exhausted":
                break
    
    # Summary
    elapsed = time.time() - start_time
    
    log.info("="*60)
    log.info("FETCH SUMMARY")
    log.info("="*60)
    log.info(f"Runtime: {elapsed/60:.1f} minutes")
    log.info(f"Channels processed: {total_stats['channels']}")
    log.info(f"Channels skipped: {total_stats['channels_skipped']}")
    log.info(f"Videos fetched: {total_stats['videos']} ({total_stats['videos_new']} new)")
    log.info(f"Video stats updated: {total_stats['videos_stats_updated']}")
    log.info(f"Comments fetched: {total_stats['comments']}")
    log.info(f"Transcripts fetched: {total_stats['transcripts']}")
    log.info(f"Errors: {total_stats['errors']}")
    
    quota.log_summary()
    
    # Set GitHub Actions outputs
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"channels={total_stats['channels']}\n")
            f.write(f"videos={total_stats['videos']}\n")
            f.write(f"videos_new={total_stats['videos_new']}\n")
            f.write(f"comments={total_stats['comments']}\n")
            f.write(f"transcripts={total_stats['transcripts']}\n")


if __name__ == "__main__":
    main()
```

## Section 8: Local Transcript Fetcher — `scripts/local/fetch_transcripts.py`

This script lives in a `local/` subdirectory as a deliberate reminder: **it cannot run in GitHub Actions or any cloud environment** because YouTube detects and blocks transcript requests from datacentre IP ranges.

It uses the `youtube-transcript-api` library (separate from the YouTube Data API v3) which scrapes transcript data directly. The script:

1. Queries the database for all videos that don't have a transcript yet
2. Iterates through them with a **1-second rate limit** between calls
3. Tries three strategies in order:
   - Manually created transcript (highest quality)
   - Auto-generated transcript
   - Translated to English
4. On failure, records the *reason* in the `transcripts` table (`no_transcript_available`, `transcripts_disabled`, `video_unavailable`) so the video is not retried indefinitely

This distinction — collecting data in the cloud vs. local-only transcript fetching — is one of the key architectural decisions of the project.

```bash
sed -n '1,100p' scripts/local/fetch_transcripts.py
```

```output
#!/usr/bin/env python3
"""
Standalone transcript fetching utility.

This script fetches transcripts for videos in the database that don't have them yet.
It must be run locally (not in CI/cloud) because YouTube blocks transcript requests
from cloud IP addresses.

Features:
- Respectful rate limiting (1 request per second by default)
- Retry logic with exponential backoff
- Progress tracking and resumability
- Channel or video-specific fetching
- Detailed failure reason tracking

Usage:
    # Fetch transcripts for all videos without them
    python fetch_transcripts.py
    
    # Fetch for specific channel
    python fetch_transcripts.py --channel @samwitteveenai
    python fetch_transcripts.py --channel UC55ODQSvARtgSyc8ThfiepQ
    
    # Fetch for specific videos
    python fetch_transcripts.py --video VIDEO_ID1 VIDEO_ID2
    
    # Limit number to fetch
    python fetch_transcripts.py --limit 100
    
    # Adjust rate limiting
    python fetch_transcripts.py --delay 2.0  # 2 seconds between requests
    
    # Test mode (don't save to database)
    python fetch_transcripts.py --dry-run --limit 5

Environment Variables:
    TURSO_DATABASE_URL: Database connection URL
    TURSO_AUTH_TOKEN: Database auth token (for Turso cloud)
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Optional

# Add scripts directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger
from database import get_connection, init_database, insert_transcript

log = get_logger("fetch_transcripts")

# ============================================================================
# TRANSCRIPT API SETUP
# ============================================================================

try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import (
        NoTranscriptFound,
        TranscriptsDisabled,
        VideoUnavailable,
    )
    TRANSCRIPT_API_AVAILABLE = True
    log.debug("youtube-transcript-api loaded successfully")
except ImportError:
    TRANSCRIPT_API_AVAILABLE = False
    log.error("youtube-transcript-api not installed! Install with: pip install youtube-transcript-api")

# Default rate limiting
DEFAULT_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 5.0  # seconds before retry
PROGRESS_LOG_INTERVAL = 50  # Log progress every N videos

# Singleton API instance
_api_instance = None


def get_transcript_api():
    """Get or create the YouTubeTranscriptApi instance."""
    global _api_instance
    if _api_instance is None and TRANSCRIPT_API_AVAILABLE:
        _api_instance = YouTubeTranscriptApi()
    return _api_instance


# ============================================================================
# TRANSCRIPT FETCHING
# ============================================================================

def fetch_transcript(video_id: str, languages: list = None) -> dict:
    """
    Fetch transcript for a video using youtube-transcript-api v1.x.
    
    Priority:
    1. Manually created transcript in requested languages
```

```bash
grep -n 'def \|NoTranscript\|TranscriptsDisabled\|VideoUnavailable\|no_transcript\|transcripts_disabled\|video_unavailable' scripts/local/fetch_transcripts.py | head -40
```

```output
63:        NoTranscriptFound,
64:        TranscriptsDisabled,
65:        VideoUnavailable,
83:def get_transcript_api():
95:def fetch_transcript(video_id: str, languages: list = None) -> dict:
138:        except NoTranscriptFound:
152:            except NoTranscriptFound:
205:    except TranscriptsDisabled:
207:    except VideoUnavailable:
219:def fetch_transcript_with_retry(
286:def get_videos_needing_transcripts(conn, channel_id: str = None, limit: int = None) -> list:
326:def _escape_like_pattern(value: str) -> str:
340:def resolve_channel_id(conn, identifier: str) -> Optional[str]:
370:def main():
```

```bash
sed -n '95,230p' scripts/local/fetch_transcripts.py
```

```output
def fetch_transcript(video_id: str, languages: list = None) -> dict:
    """
    Fetch transcript for a video using youtube-transcript-api v1.x.
    
    Priority:
    1. Manually created transcript in requested languages
    2. Auto-generated transcript in requested languages  
    3. Any transcript translated to English
    
    Args:
        video_id: YouTube video ID
        languages: List of language codes in priority order (default: ['en', 'en-US', 'en-GB'])
    
    Returns:
        Dict with transcript data or availability status
    """
    if languages is None:
        languages = ['en', 'en-US', 'en-GB']
    
    if not TRANSCRIPT_API_AVAILABLE:
        return {"available": False, "reason": "youtube-transcript-api not installed"}
    
    try:
        api = get_transcript_api()
        if api is None:
            return {"available": False, "reason": "Transcript API not available"}
        
        # Get list of available transcripts
        transcript_list = api.list(video_id)
        
        transcript = None
        transcript_info = {}
        
        # Strategy 1: Try to find manually created transcript in preferred languages
        try:
            transcript = transcript_list.find_manually_created_transcript(languages)
            transcript_info = {
                "transcript_type": "manual",
                "language": transcript.language,
                "language_code": transcript.language_code,
                "is_generated": False,
            }
            log.debug(f"Found manual transcript: {transcript.language}")
        except NoTranscriptFound:
            log.debug("No manual transcript in preferred languages")
        
        # Strategy 2: Try auto-generated transcript
        if not transcript:
            try:
                transcript = transcript_list.find_generated_transcript(languages)
                transcript_info = {
                    "transcript_type": "auto-generated",
                    "language": transcript.language,
                    "language_code": transcript.language_code,
                    "is_generated": True,
                }
                log.debug(f"Found auto-generated transcript: {transcript.language}")
            except NoTranscriptFound:
                log.debug("No auto-generated transcript in preferred languages")
        
        # Strategy 3: Translate any available transcript to English
        if not transcript:
            for t in transcript_list:
                if t.is_translatable:
                    try:
                        transcript = t.translate('en')
                        transcript_info = {
                            "transcript_type": "translated",
                            "language": "English (translated)",
                            "language_code": "en",
                            "is_generated": t.is_generated,
                            "original_language": t.language,
                            "original_language_code": t.language_code,
                        }
                        log.debug(f"Translating from {t.language} to English")
                        break
                    except Exception as e:
                        log.debug(f"Translation failed for {t.language}: {e}")
                        continue
        
        if not transcript:
            return {"available": False, "reason": "No transcript in supported languages"}
        
        # Fetch the actual transcript content
        fetched = transcript.fetch()
        
        # Convert to raw data format (list of dicts)
        raw_data = fetched.to_raw_data()
        
        # Build entries with computed end times
        entries = []
        full_text_parts = []
        
        for entry in raw_data:
            entries.append({
                "start": entry["start"],
                "duration": entry["duration"],
                "end": entry["start"] + entry["duration"],
                "text": entry["text"]
            })
            full_text_parts.append(entry["text"])
        
        return {
            "available": True,
            **transcript_info,
            "entries": entries,
            "full_text": " ".join(full_text_parts),
            "snippet_count": len(entries),
        }
        
    except TranscriptsDisabled:
        return {"available": False, "reason": "Transcripts disabled by uploader"}
    except VideoUnavailable:
        return {"available": False, "reason": "Video unavailable"}
    except Exception as e:
        error_msg = str(e)
        # Check for IP blocking
        if any(x in error_msg.lower() for x in ['blocked', 'ip', '429', 'too many']):
            log.warning(f"Possible IP block for {video_id}: {e}")
            return {"available": False, "reason": f"Request blocked: {error_msg}"}
        log.debug(f"Transcript fetch error for {video_id}: {type(e).__name__}: {e}")
        return {"available": False, "reason": str(e)}


def fetch_transcript_with_retry(
    video_id: str, 
    max_retries: int = MAX_RETRIES,
    retry_delay: float = RETRY_DELAY
) -> dict:
    """
    Fetch transcript with retry logic.
    
    Returns dict with:
        - success: bool
        - transcript: dict (if success)
        - error_type: str (if failure)
```

## Section 9: Analytics — `scripts/analyse.py`

`analyse.py` is a lightweight reporting tool that reads from the database and produces formatted output. It offers four preset reports plus an escape hatch for arbitrary SQL:

| Report | What it shows |
|--------|--------------|
| `summary` | Total counts across all tables |
| `channels` | Latest channel stats (subscribers, views) |
| `growth` | 7-day view growth per video |
| `top-videos` | Videos ranked by current view count |
| Custom SQL | Pass any `--sql` query |

Optional `--output` flag exports results to CSV.

```bash
cat scripts/analyse.py
```

```output
#!/usr/bin/env python3
"""
YouTube Analytics Query Tool

Pre-built queries for analyzing YouTube channel data.
Can also run custom SQL queries against the database.

Usage:
    # Run a preset report
    python analyze.py --report growth
    python analyze.py --report top-videos
    python analyze.py --report comment-velocity
    
    # Custom SQL query
    python analyze.py --sql "SELECT * FROM channels"
    
    # Export query results
    python analyze.py --report growth --output growth.csv
"""

import argparse
import sys
from datetime import datetime, timedelta

from database import get_connection


REPORTS = {
    "summary": {
        "description": "Overall database summary",
        "query": """
            SELECT 
                (SELECT COUNT(*) FROM channels) as total_channels,
                (SELECT COUNT(*) FROM videos) as total_videos,
                (SELECT COUNT(*) FROM comments) as total_comments,
                (SELECT COUNT(*) FROM transcripts) as total_transcripts,
                (SELECT MIN(fetched_at) FROM video_stats) as earliest_data,
                (SELECT MAX(fetched_at) FROM video_stats) as latest_data
        """
    },
    
    "channels": {
        "description": "List all tracked channels with latest stats",
        "query": """
            SELECT 
                c.title,
                c.channel_id,
                cs.subscriber_count,
                cs.view_count,
                cs.video_count,
                cs.fetched_at as last_updated
            FROM channels c
            JOIN channel_stats cs ON c.channel_id = cs.channel_id
            WHERE cs.fetched_at = (
                SELECT MAX(fetched_at) FROM channel_stats WHERE channel_id = c.channel_id
            )
            ORDER BY cs.subscriber_count DESC
        """
    },
    
    "growth": {
        "description": "Video view growth over last 7 days",
        "query": """
            WITH recent_stats AS (
                SELECT 
                    video_id,
                    MIN(view_count) as views_start,
                    MAX(view_count) as views_end
                FROM video_stats
                WHERE fetched_at >= datetime('now', '-7 days')
                GROUP BY video_id
                HAVING COUNT(*) >= 2
            )
            SELECT 
                v.title,
                c.title as channel,
                rs.views_end - rs.views_start as view_growth,
                rs.views_start as views_7d_ago,
                rs.views_end as views_now,
                ROUND(100.0 * (rs.views_end - rs.views_start) / MAX(rs.views_start, 1), 2) as growth_pct,
                v.published_at
            FROM recent_stats rs
            JOIN videos v ON rs.video_id = v.video_id
            JOIN channels c ON v.channel_id = c.channel_id
            WHERE rs.views_end > rs.views_start
            ORDER BY view_growth DESC
            LIMIT 50
        """
    },
    
    "top-videos": {
        "description": "Top videos by current view count",
        "query": """
            SELECT 
                v.title,
                c.title as channel,
                vs.view_count,
                vs.like_count,
                vs.comment_count,
                v.published_at,
                v.duration_seconds / 60 as duration_mins
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            JOIN video_stats vs ON v.video_id = vs.video_id
            WHERE vs.fetched_at = (
                SELECT MAX(fetched_at) FROM video_stats WHERE video_id = v.video_id
            )
            ORDER BY vs.view_count DESC
            LIMIT 50
        """
    },
    
    "recent-uploads": {
        "description": "Most recent video uploads across all channels",
        "query": """
            SELECT 
                v.title,
                c.title as channel,
                v.published_at,
                vs.view_count,
                vs.like_count,
                v.duration_seconds / 60 as duration_mins,
                CASE WHEN t.video_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_transcript
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            LEFT JOIN video_stats vs ON v.video_id = vs.video_id 
                AND vs.fetched_at = (SELECT MAX(fetched_at) FROM video_stats WHERE video_id = v.video_id)
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            ORDER BY v.published_at DESC
            LIMIT 50
        """
    },
    
    "comment-velocity": {
        "description": "Videos with highest comment activity (last 7 days)",
        "query": """
            SELECT 
                v.title,
                c.title as channel,
                COUNT(*) as new_comments_7d,
                ROUND(COUNT(*) / 7.0, 1) as comments_per_day,
                v.published_at
            FROM comments cm
            JOIN videos v ON cm.video_id = v.video_id
            JOIN channels c ON v.channel_id = c.channel_id
            WHERE cm.published_at >= datetime('now', '-7 days')
            GROUP BY v.video_id, v.title, c.title, v.published_at
            ORDER BY new_comments_7d DESC
            LIMIT 30
        """
    },
    
    "engagement-rate": {
        "description": "Videos by engagement rate (likes + comments / views)",
        "query": """
            SELECT 
                v.title,
                c.title as channel,
                vs.view_count,
                vs.like_count,
                vs.comment_count,
                ROUND(100.0 * (vs.like_count + vs.comment_count) / MAX(vs.view_count, 1), 4) as engagement_rate,
                v.published_at
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            JOIN video_stats vs ON v.video_id = vs.video_id
            WHERE vs.fetched_at = (
                SELECT MAX(fetched_at) FROM video_stats WHERE video_id = v.video_id
            )
            AND vs.view_count > 1000
            ORDER BY engagement_rate DESC
            LIMIT 50
        """
    },
    
    "subscriber-growth": {
        "description": "Channel subscriber growth over time",
        "query": """
            SELECT 
                c.title as channel,
                DATE(cs.fetched_at) as day,
                MAX(cs.subscriber_count) as subscribers
            FROM channel_stats cs
            JOIN channels c ON cs.channel_id = c.channel_id
            WHERE cs.fetched_at >= datetime('now', '-30 days')
            GROUP BY c.channel_id, c.title, DATE(cs.fetched_at)
            ORDER BY c.title, day
        """
    },
    
    "transcript-coverage": {
        "description": "Transcript availability by channel",
        "query": """
            SELECT 
                c.title as channel,
                COUNT(v.video_id) as total_videos,
                COUNT(t.video_id) as with_transcript,
                ROUND(100.0 * COUNT(t.video_id) / MAX(COUNT(v.video_id), 1), 1) as coverage_pct
            FROM channels c
            JOIN videos v ON c.channel_id = v.channel_id
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            GROUP BY c.channel_id, c.title
            ORDER BY total_videos DESC
        """
    },
    
    "popular-commenters": {
        "description": "Most active commenters across all channels",
        "query": """
            SELECT 
                author_display_name,
                author_channel_id,
                COUNT(*) as comment_count,
                COUNT(DISTINCT video_id) as videos_commented,
                SUM(like_count) as total_likes_received,
                MIN(published_at) as first_comment,
                MAX(published_at) as last_comment
            FROM comments
            WHERE author_display_name IS NOT NULL
            GROUP BY author_display_name, author_channel_id
            HAVING COUNT(*) >= 5
            ORDER BY comment_count DESC
            LIMIT 50
        """
    },
    
    "video-length-performance": {
        "description": "Performance by video length buckets",
        "query": """
            SELECT 
                CASE 
                    WHEN duration_seconds < 60 THEN '< 1 min'
                    WHEN duration_seconds < 300 THEN '1-5 min'
                    WHEN duration_seconds < 600 THEN '5-10 min'
                    WHEN duration_seconds < 1200 THEN '10-20 min'
                    WHEN duration_seconds < 3600 THEN '20-60 min'
                    ELSE '60+ min'
                END as length_bucket,
                COUNT(*) as video_count,
                ROUND(AVG(vs.view_count)) as avg_views,
                ROUND(AVG(vs.like_count)) as avg_likes,
                ROUND(AVG(100.0 * vs.like_count / MAX(vs.view_count, 1)), 2) as avg_like_rate
            FROM videos v
            JOIN video_stats vs ON v.video_id = vs.video_id
            WHERE vs.fetched_at = (
                SELECT MAX(fetched_at) FROM video_stats WHERE video_id = v.video_id
            )
            AND v.duration_seconds IS NOT NULL
            GROUP BY length_bucket
            ORDER BY 
                CASE length_bucket
                    WHEN '< 1 min' THEN 1
                    WHEN '1-5 min' THEN 2
                    WHEN '5-10 min' THEN 3
                    WHEN '10-20 min' THEN 4
                    WHEN '20-60 min' THEN 5
                    ELSE 6
                END
        """
    },
    
    "fetch-history": {
        "description": "Recent fetch operations log",
        "query": """
            SELECT 
                f.fetch_id,
                c.title as channel,
                f.fetch_type,
                f.started_at,
                f.completed_at,
                f.videos_fetched,
                f.comments_fetched,
                f.transcripts_fetched,
                f.status,
                f.errors
            FROM fetch_log f
            LEFT JOIN channels c ON f.channel_id = c.channel_id
            ORDER BY f.started_at DESC
            LIMIT 50
        """
    },
}


def run_query(conn, query: str, allow_unsafe: bool = False) -> list:
    """
    Run a SQL query and return results.

    Args:
        conn: Database connection
        query: SQL query string
        allow_unsafe: If False, only SELECT queries are allowed (default: False)

    Returns:
        List of result rows

    Raises:
        ValueError: If query is not a SELECT and allow_unsafe is False
    """
    # Security: Only allow SELECT queries by default to prevent accidental data modification
    query_upper = query.strip().upper()
    if not allow_unsafe and not query_upper.startswith("SELECT"):
        raise ValueError(
            "Only SELECT queries are allowed for safety. "
            "Use --unsafe flag to allow other query types."
        )
    return conn.execute(query).fetchall()


def get_column_names(conn, query: str) -> list[str]:
    """Get column names for a query."""
    result = conn.execute(query)
    return [desc[0] for desc in result.description]


def print_table(headers: list[str], rows: list, max_width: int = 50):
    """Print results as a formatted table."""
    if not rows:
        print("No results")
        return
    
    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            val_str = str(val) if val is not None else "NULL"
            if len(val_str) > max_width:
                val_str = val_str[:max_width-3] + "..."
            widths[i] = max(widths[i], len(val_str))
    
    # Print header
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))
    
    # Print rows
    for row in rows:
        row_strs = []
        for i, val in enumerate(row):
            val_str = str(val) if val is not None else "NULL"
            if len(val_str) > max_width:
                val_str = val_str[:max_width-3] + "..."
            row_strs.append(val_str.ljust(widths[i]))
        print(" | ".join(row_strs))


def export_csv(headers: list[str], rows: list, output_path: str):
    """Export results to CSV."""
    import csv
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Exported to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Run analytics queries on YouTube data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Available reports:\n" + "\n".join(
            f"  {name}: {info['description']}" 
            for name, info in REPORTS.items()
        )
    )
    
    parser.add_argument(
        "--report", "-r",
        choices=list(REPORTS.keys()),
        help="Run a preset report"
    )
    parser.add_argument(
        "--sql", "-s",
        help="Run custom SQL query"
    )
    parser.add_argument(
        "--output", "-o",
        help="Export results to CSV file"
    )
    parser.add_argument(
        "--list-reports",
        action="store_true",
        help="List available reports"
    )
    parser.add_argument(
        "--unsafe",
        action="store_true",
        help="Allow non-SELECT queries (INSERT, UPDATE, DELETE) - use with caution"
    )

    args = parser.parse_args()
    
    if args.list_reports:
        print("Available reports:\n")
        for name, info in REPORTS.items():
            print(f"  {name}")
            print(f"    {info['description']}\n")
        return
    
    if not args.report and not args.sql:
        parser.print_help()
        print("\nError: Must specify --report or --sql")
        sys.exit(1)
    
    # Connect to database
    conn = get_connection()
    
    # Get query
    if args.report:
        query = REPORTS[args.report]["query"]
        print(f"Report: {args.report}")
        print(f"{REPORTS[args.report]['description']}\n")
    else:
        query = args.sql
    
    # Run query
    try:
        # Preset reports are always safe (SELECT only), custom SQL needs --unsafe for non-SELECT
        allow_unsafe = args.unsafe if args.sql else True
        headers = get_column_names(conn, query)
        rows = run_query(conn, query, allow_unsafe=allow_unsafe)

        if args.output:
            export_csv(headers, rows, args.output)
        else:
            print_table(headers, rows)
            print(f"\n({len(rows)} rows)")

    except ValueError as e:
        # Security restriction error
        print(f"Security error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Query error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

## Section 10: Automation — GitHub Actions

The `.github/workflows/` directory holds the scheduled jobs. The main one, `fetch.yml`, is the heartbeat of the system.

```bash
ls .github/workflows/
```

```output
database-sync-v2.yml
database-sync.yml
db-maintenance.yml
fetch.yml
optimize-database.yml
```

```bash
cat .github/workflows/fetch.yml
```

```output
name: Fetch YouTube Metadata

on:
  # Manual trigger
  workflow_dispatch:
    inputs:
      channel:
        description: 'Single channel to fetch (leave empty to use config)'
        required: false
        type: string
      backfill:
        description: 'Run full backfill (ignore incremental)'
        required: false
        type: boolean
        default: false
      max_videos:
        description: 'Max videos per channel (default: unlimited)'
        required: false
        type: number
      max_video_age:
        description: 'Max video age in days (default: unlimited)'
        required: false
        type: number
      skip_comments:
        description: 'Skip fetching comments'
        required: false
        type: boolean
        default: false
      min_new_comments:
        description: 'Skip videos with fewer than N new comments (default: 10, set to 0 to disable)'
        required: false
        type: number
        default: 10
      reset_quota:
        description: 'Reset quota counter (use if quota tracking was corrupted)'
        required: false
        type: boolean
        default: false
      log_level:
        description: 'Console log level'
        required: false
        type: choice
        options:
          - INFO
          - DEBUG
          - WARNING
        default: INFO
      progress_log_interval:
        description: 'Log progress every N batches (1 = every batch)'
        required: false
        type: number
        default: 10

  # Scheduled runs - 4 times daily
  schedule:
    - cron: '0 0 * * *'   # Midnight UTC
    - cron: '0 6 * * *'   # 6 AM UTC
    - cron: '0 12 * * *'  # Noon UTC
    - cron: '0 18 * * *'  # 6 PM UTC

env:
  PYTHON_VERSION: '3.11'

jobs:
  fetch:
    runs-on: ubuntu-latest
    
    # Prevent concurrent runs to avoid database conflicts
    concurrency:
      group: youtube-fetch
      cancel-in-progress: false
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: 'pip'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Ensure config exists
        run: |
          if [ ! -f "config/channels.yaml" ]; then
            if [ -f "config/channels.example.yaml" ]; then
              cp config/channels.example.yaml config/channels.yaml
              echo "Using example config"
            else
              echo "No config file found!"
              exit 1
            fi
          fi

      - name: Fetch YouTube data
        id: fetch
        env:
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          POSTGRES_URL: ${{ secrets.POSTGRES_URL }}
          TURSO_DATABASE_URL: ${{ secrets.TURSO_DATABASE_URL }}
          TURSO_AUTH_TOKEN: ${{ secrets.TURSO_AUTH_TOKEN }}
          CONSOLE_LOG_LEVEL: ${{ github.event.inputs.log_level || 'INFO' }}
          PROGRESS_LOG_INTERVAL: ${{ github.event.inputs.progress_log_interval || '10' }}
        run: |
          cd scripts
          
          # Build command
          if [ -n "${{ github.event.inputs.channel }}" ]; then
            CMD="python fetch.py --channel '${{ github.event.inputs.channel }}'"
          else
            CMD="python fetch.py --config ../config/channels.yaml"
          fi
          
          # Add flags
          if [ "${{ github.event.inputs.backfill }}" == "true" ]; then
            CMD="$CMD --backfill"
          fi

          # Convert numeric inputs to integers (iOS GitHub app sends floats like "10.0")
          if [ -n "${{ github.event.inputs.max_videos }}" ]; then
            MAX_VIDEOS=$(printf "%.0f" "${{ github.event.inputs.max_videos }}")
            CMD="$CMD --max-videos $MAX_VIDEOS"
          fi

          if [ -n "${{ github.event.inputs.max_video_age }}" ]; then
            MAX_VIDEO_AGE=$(printf "%.0f" "${{ github.event.inputs.max_video_age }}")
            CMD="$CMD --max-video-age $MAX_VIDEO_AGE"
          fi

          if [ "${{ github.event.inputs.skip_comments }}" == "true" ]; then
            CMD="$CMD --skip-comments"
          fi

          if [ -n "${{ github.event.inputs.min_new_comments }}" ]; then
            MIN_NEW_COMMENTS=$(printf "%.0f" "${{ github.event.inputs.min_new_comments }}")
            CMD="$CMD --min-new-comments $MIN_NEW_COMMENTS"
          fi

          if [ "${{ github.event.inputs.reset_quota }}" == "true" ]; then
            CMD="$CMD --reset-quota"
          fi
          
          echo "Running: $CMD"
          eval $CMD

      - name: Generate summary report
        if: always()
        env:
          POSTGRES_URL: ${{ secrets.POSTGRES_URL }}
          TURSO_DATABASE_URL: ${{ secrets.TURSO_DATABASE_URL }}
          TURSO_AUTH_TOKEN: ${{ secrets.TURSO_AUTH_TOKEN }}
        run: |
          cd scripts
          
          echo "## YouTube Fetch Results" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "**Run time:** $(date -u)" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "**Note:** Transcripts must be fetched locally with \`fetch_transcripts.py\`" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          
          # Summary stats
          echo "### Database Summary" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
          python analyse.py --report summary >> $GITHUB_STEP_SUMMARY || echo "Could not generate summary"
          echo '```' >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          
          # Channels
          echo "### Tracked Channels" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
          python analyse.py --report channels >> $GITHUB_STEP_SUMMARY || echo "Could not list channels"
          echo '```' >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          
          # Recent fetch history
          echo "### Recent Fetches" >> $GITHUB_STEP_SUMMARY
          echo '```' >> $GITHUB_STEP_SUMMARY
          python analyse.py --report fetch-history >> $GITHUB_STEP_SUMMARY || echo "Could not get fetch history"
          echo '```' >> $GITHUB_STEP_SUMMARY

      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: fetch-logs-${{ github.run_number }}
          path: scripts/logs/
          retention-days: 30
```

Key points in `fetch.yml`:

- **`concurrency: group: youtube-fetch`** — only one instance runs at a time; if a new scheduled run starts while one is in progress it waits rather than cancelling the running job (`cancel-in-progress: false`).
- **Secrets are passed as environment variables** — the workflow never writes tokens to disk.
- **The summary step runs `if: always()`** — even if the fetch fails it still writes a report to the GitHub Actions step summary page and retries the database query.
- **Logs are uploaded as 30-day artefacts** — the full DEBUG log file from every run is preserved for debugging.
- **Transcript note** — the summary step reminds the operator that transcripts need a separate local run.

The other workflows (`database-sync.yml`, `db-maintenance.yml`, `optimize-database.yml`) handle one-off operations: migrating between database backends, running health checks, and running `VACUUM`/`ANALYZE` on the database.

## Walkthrough Complete

Here's a one-page summary of the full code flow:

    ┌─────────────────────────────────────────────────────────────┐
    │  GitHub Actions / CLI                                        │
    │  python fetch.py --config config/channels.yaml              │
    └─────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
    ┌─────────────────────────────────────────────────────────────┐
    │  config.py  →  load_config()                                │
    │  logger.py  →  setup_logging()                              │
    │  database.py → get_connection() + init_database()           │
    │  quota.py   →  QuotaTracker (loads today's used count)      │
    └─────────────────┬───────────────────────────────────────────┘
                      │ for each channel in channels.yaml
                      ▼
    ┌─────────────────────────────────────────────────────────────┐
    │  fetch.py: fetch_channel_data()                             │
    │                                                             │
    │  Phase 1: YouTubeFetcher.fetch_channel()                    │
    │           → upsert_channel() + insert_channel_stats()       │
    │                                                             │
    │  Phase 2: YouTubeFetcher.search_channel_videos()            │
    │           OR fetch_playlist_video_ids() for backfill        │
    │                                                             │
    │  Phase 3: YouTubeFetcher.fetch_videos()  [batched]          │
    │           → upsert_videos_batch() + insert_video_stats()    │
    │                                                             │
    │  Phase 4: fetch_comments_parallel()  [ThreadPoolExecutor]   │
    │           → YouTubeFetcher.fetch_comments()  (per thread)   │
    │           → insert_comments()                               │
    └─────────────────┬───────────────────────────────────────────┘
                      │ locally (not in CI)
                      ▼
    ┌─────────────────────────────────────────────────────────────┐
    │  local/fetch_transcripts.py                                 │
    │  → youtube_transcript_api  → insert_transcript()            │
    └─────────────────────────────────────────────────────────────┘
                      │ for reporting
                      ▼
    ┌─────────────────────────────────────────────────────────────┐
    │  analyse.py  → preset SQL reports or custom --sql           │
    └─────────────────────────────────────────────────────────────┘

Every layer above writes through `database.py`'s retry-wrapped functions into the same schema, building a time-series record of YouTube channel activity.
