"""
Centralized configuration management for YouTube metadata fetcher.

Configuration is loaded from multiple sources with the following priority:
1. Config file (channels.yaml settings section) - highest priority for non-secrets
2. Environment variables - required for secrets, fallback for other settings
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


@dataclass
class Config:
    """
    Configuration container with typed access to all settings.

    Settings are loaded from config file with environment variable fallbacks.
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
    """Load settings section from YAML config file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config.get("settings", {})
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Warning: Could not load config file {config_path}: {e}")
        return {}


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

    # Helper to get setting with priority: yaml > env > default
    def get_setting(yaml_key: str, env_key: str, default, cast_type=None):
        # First check YAML
        if yaml_key in yaml_settings:
            value = yaml_settings[yaml_key]
            if cast_type is not None:
                try:
                    return cast_type(value)
                except (ValueError, TypeError):
                    pass
            return value
        # Then check env
        return _get_env_or_default(env_key, default, cast_type)

    # Build config object
    config = Config(
        # Database settings - URL from config, tokens from env
        database_backend=get_setting("database_backend", "DATABASE_BACKEND", DEFAULTS["database_backend"]),
        database_url=get_setting("database_url", "TURSO_DATABASE_URL", DEFAULTS["database_url"]),
        database_auth_token=os.environ.get("TURSO_AUTH_TOKEN", ""),  # Always from env
        postgres_url=os.environ.get("POSTGRES_URL", ""),  # Always from env

        # Logging settings
        log_dir=get_setting("log_dir", "LOG_DIR", DEFAULTS["log_dir"]),
        log_level=get_setting("log_level", "LOG_LEVEL", DEFAULTS["log_level"]),
        console_log_level=get_setting("console_log_level", "CONSOLE_LOG_LEVEL", DEFAULTS["console_log_level"]),

        # API retry settings
        api_max_retries=get_setting("api_max_retries", "API_MAX_RETRIES", DEFAULTS["api_max_retries"], int),
        api_base_delay=get_setting("api_base_delay", "API_BASE_DELAY", DEFAULTS["api_base_delay"], float),
        api_max_delay=get_setting("api_max_delay", "API_MAX_DELAY", DEFAULTS["api_max_delay"], float),

        # Database retry settings
        db_max_retries=get_setting("db_max_retries", "DB_MAX_RETRIES", DEFAULTS["db_max_retries"], int),
        db_base_delay=get_setting("db_base_delay", "DB_BASE_DELAY", DEFAULTS["db_base_delay"], float),
        db_max_delay=get_setting("db_max_delay", "DB_MAX_DELAY", DEFAULTS["db_max_delay"], float),
        db_exponential_base=get_setting("db_exponential_base", "DB_EXPONENTIAL_BASE", DEFAULTS["db_exponential_base"], float),

        # Runtime limits
        max_runtime_minutes=get_setting("max_runtime_minutes", "MAX_RUNTIME_MINUTES", DEFAULTS["max_runtime_minutes"], int),
        default_batch_size=get_setting("default_batch_size", "DEFAULT_BATCH_SIZE", DEFAULTS["default_batch_size"], int),

        # Quota settings
        quota_limit=get_setting("quota_limit", "YOUTUBE_QUOTA_LIMIT", DEFAULTS["quota_limit"], int),
        quota_warn_threshold=get_setting("quota_warn_threshold", "QUOTA_WARN_THRESHOLD", DEFAULTS["quota_warn_threshold"], float),
        quota_abort_threshold=get_setting("quota_abort_threshold", "QUOTA_ABORT_THRESHOLD", DEFAULTS["quota_abort_threshold"], float),
        quota_checkpoint_threshold=get_setting("quota_checkpoint_threshold", "QUOTA_CHECKPOINT_THRESHOLD", DEFAULTS["quota_checkpoint_threshold"], int),

        # Performance tuning
        default_comment_workers=get_setting("default_comment_workers", "DEFAULT_COMMENT_WORKERS", DEFAULTS["default_comment_workers"], int),
        progress_log_interval=get_setting("progress_log_interval", "PROGRESS_LOG_INTERVAL", DEFAULTS["progress_log_interval"], int),
        progress_callback_interval=get_setting("progress_callback_interval", "PROGRESS_CALLBACK_INTERVAL", DEFAULTS["progress_callback_interval"], int),
        checkpoint_slow_threshold_ms=get_setting("checkpoint_slow_threshold_ms", "CHECKPOINT_SLOW_THRESHOLD_MS", DEFAULTS["checkpoint_slow_threshold_ms"], int),

        # API constants
        api_max_results_per_page=get_setting("api_max_results_per_page", "API_MAX_RESULTS_PER_PAGE", DEFAULTS["api_max_results_per_page"], int),
        search_pagination_limit=get_setting("search_pagination_limit", "SEARCH_PAGINATION_LIMIT", DEFAULTS["search_pagination_limit"], int),

        _config_file=config_path,
    )

    return config


def get_config(config_path: Optional[str] = None, reload: bool = False) -> Config:
    """
    Get the global configuration instance.

    Uses singleton pattern - loads config once and reuses it.

    Args:
        config_path: Path to config file (only used on first load or reload)
        reload: If True, force reload of configuration

    Returns:
        Config object with all settings
    """
    global _config

    if _config is None or reload:
        _config = load_config(config_path)

    return _config


def set_config(config: Config) -> None:
    """
    Set the global configuration instance.

    Useful for testing or when config needs to be set programmatically.
    """
    global _config
    _config = config
