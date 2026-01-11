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

    Provides resilient execute() and commit() methods that handle
    transient Turso errors with exponential backoff. Automatically
    refreshes the connection on stream errors.
    """

    def __init__(self, conn, url: str = None, auth_token: str = None):
        self._conn = conn
        self._url = url
        self._auth_token = auth_token
        self._in_transaction = False
        self._lock = threading.Lock()  # Protect connection refresh

    def _refresh_connection(self):
        """Recreate the underlying connection after stream errors."""
        if not self._url:
            log.warning("Cannot refresh connection: no URL stored")
            return

        import libsql

        log.info("Refreshing database connection after stream error")
        with self._lock:
            if self._url.startswith("libsql://") or self._url.startswith("https://"):
                self._conn = libsql.connect(database=self._url, auth_token=self._auth_token)
            else:
                self._conn = libsql.connect(database=self._url)

    def _execute_with_refresh(self, method_name: str, *args, **kwargs):
        """Execute an operation with connection refresh on stream errors.

        IMPORTANT: Takes method_name (string) instead of bound method to ensure
        we use the NEW connection after refresh, not the old captured one.
        """
        # Get config values for retry logic
        max_retries = get_db_max_retries()
        base_delay = get_db_base_delay()
        max_delay = get_db_max_delay()
        exp_base = get_db_exponential_base()

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                # Get fresh method reference from current connection each attempt
                method = getattr(self._conn, method_name)
                return method(*args, **kwargs)
            except Exception as e:
                if needs_connection_refresh(e):
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exp_base ** attempt), max_delay)
                        log.warning(f"Stream error, refreshing connection in {delay:.1f}s "
                                   f"(attempt {attempt + 1}/{max_retries + 1}): {e}")
                        time.sleep(delay)
                        self._refresh_connection()
                        continue
                    else:
                        log.error(f"Stream error persists after {max_retries + 1} attempts: {e}")
                        raise
                elif is_retryable_error(e):
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exp_base ** attempt), max_delay)
                        log.warning(f"Database error (attempt {attempt + 1}/{max_retries + 1}), "
                                   f"retrying in {delay:.1f}s: {e}")
                        time.sleep(delay)
                        continue
                    else:
                        log.error(f"Database operation failed after {max_retries + 1} attempts: {e}")
                        raise
                else:
                    log.error(f"Non-retryable database error: {e}")
                    raise

        raise last_exception

    def execute(self, sql: str, parameters: tuple = None):
        """Execute SQL with automatic retry and connection refresh on stream errors."""
        if parameters:
            return self._execute_with_refresh("execute", sql, parameters)
        return self._execute_with_refresh("execute", sql)

    def commit(self):
        """Commit transaction with automatic retry and connection refresh."""
        return self._execute_with_refresh("commit")

    def executemany(self, sql: str, parameters_list: list):
        """Execute SQL with multiple parameter sets (batch operation)."""
        return self._execute_with_refresh("executemany", sql, parameters_list)

    def __getattr__(self, name):
        """Delegate other attributes to underlying connection."""
        return getattr(self._conn, name)


class PostgresConnection:
    """
    Wrapper around psycopg connection with automatic retry logic.

    Provides resilient execute() and commit() methods that handle
    transient PostgreSQL errors with exponential backoff.

    Features:
    - Converts ? placeholders to %s for PostgreSQL compatibility
    - Thread-safe with connection pooling
    - Automatic retry on transient errors
    """

    # PostgreSQL-specific retryable error patterns
    PG_RETRYABLE_PATTERNS = [
        "connection refused",
        "connection reset",
        "connection timed out",
        "too many connections",
        "server closed the connection",
        "SSL connection has been closed",
        "could not connect to server",
        "temporary failure",
    ]

    def __init__(self, conn_string: str):
        self._conn_string = conn_string
        self._conn = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self):
        """Create a new connection."""
        import psycopg

        log.debug("Creating PostgreSQL connection")
        self._conn = psycopg.connect(self._conn_string, autocommit=False)

    def _is_retryable(self, error: Exception) -> bool:
        """Check if error is retryable."""
        error_str = str(error).lower()
        for pattern in self.PG_RETRYABLE_PATTERNS:
            if pattern in error_str:
                return True
        return False

    def _convert_sql(self, sql: str) -> str:
        """Convert SQLite SQL syntax to PostgreSQL."""
        # Convert ? placeholders to %s
        result = sql.replace("?", "%s")

        # Convert INSERT OR IGNORE to INSERT ... ON CONFLICT DO NOTHING
        # SQLite: INSERT OR IGNORE INTO table ...
        # PostgreSQL: INSERT INTO table ... ON CONFLICT DO NOTHING
        if "INSERT OR IGNORE" in result.upper():
            result = result.replace("INSERT OR IGNORE", "INSERT")
            result = result.replace("insert or ignore", "INSERT")
            # Add ON CONFLICT DO NOTHING before the final closing
            # This handles both single-row and batch inserts
            if "ON CONFLICT" not in result.upper():
                # Find the end of VALUES clause
                result = result.rstrip()
                if result.endswith(")"):
                    result += " ON CONFLICT DO NOTHING"

        # Convert SQLite datetime() function to PostgreSQL equivalents
        # datetime('now') -> NOW()
        result = re.sub(
            r"datetime\s*\(\s*'now'\s*\)",
            "NOW()",
            result,
            flags=re.IGNORECASE
        )

        # datetime('now', '-N days') -> NOW() - INTERVAL 'N days'
        result = re.sub(
            r"datetime\s*\(\s*'now'\s*,\s*'(-?\d+)\s+days'\s*\)",
            r"NOW() + INTERVAL '\1 days'",
            result,
            flags=re.IGNORECASE
        )

        # datetime('now', '-N hours') -> NOW() - INTERVAL 'N hours'
        result = re.sub(
            r"datetime\s*\(\s*'now'\s*,\s*'(-?\d+)\s+hours'\s*\)",
            r"NOW() + INTERVAL '\1 hours'",
            result,
            flags=re.IGNORECASE
        )

        # datetime('now', '-' || expr || ' hours') -> NOW() - (expr || ' hours')::INTERVAL
        # This handles dynamic interval construction
        result = re.sub(
            r"datetime\s*\(\s*'now'\s*,\s*'-'\s*\|\|\s*(.+?)\s*\|\|\s*'\s*hours\s*'\s*\)",
            r"NOW() - ((\1) || ' hours')::INTERVAL",
            result,
            flags=re.IGNORECASE
        )

        # datetime('now', '-' || expr || ' days') -> NOW() - (expr || ' days')::INTERVAL
        result = re.sub(
            r"datetime\s*\(\s*'now'\s*,\s*'-'\s*\|\|\s*(.+?)\s*\|\|\s*'\s*days\s*'\s*\)",
            r"NOW() - ((\1) || ' days')::INTERVAL",
            result,
            flags=re.IGNORECASE
        )

        # datetime(column) -> column::timestamp (cast TEXT to timestamp for comparisons)
        result = re.sub(
            r"datetime\s*\(\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\)",
            r"\1::timestamp",
            result
        )

        return result

    def _convert_placeholders(self, sql: str) -> str:
        """Convert ? placeholders to %s for PostgreSQL (legacy alias)."""
        return self._convert_sql(sql)

    def _execute_with_retry(self, method_name: str, sql: str = None, parameters=None):
        """Execute with retry logic and SQL conversion."""
        # Get config values for retry logic
        max_retries = get_db_max_retries()
        base_delay = get_db_base_delay()
        max_delay = get_db_max_delay()
        exp_base = get_db_exponential_base()

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                with self._lock:
                    if sql is not None:
                        converted_sql = self._convert_sql(sql)
                        method = getattr(self._conn, method_name)
                        if parameters:
                            return method(converted_sql, parameters)
                        return method(converted_sql)
                    else:
                        method = getattr(self._conn, method_name)
                        return method()
            except Exception as e:
                if self._is_retryable(e):
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exp_base ** attempt), max_delay)
                        log.warning(f"PostgreSQL error (attempt {attempt + 1}/{max_retries + 1}), "
                                   f"retrying in {delay:.1f}s: {e}")
                        time.sleep(delay)
                        # Reconnect on connection errors
                        try:
                            self._connect()
                        except Exception as conn_err:
                            log.warning(f"Reconnection failed: {conn_err}")
                        continue
                    else:
                        log.error(f"PostgreSQL operation failed after {max_retries + 1} attempts: {e}")
                        raise
                else:
                    log.error(f"Non-retryable PostgreSQL error: {e}")
                    # Rollback to reset connection state after non-retryable error
                    # This prevents "current transaction is aborted" cascading failures
                    try:
                        if self._conn:
                            self._conn.rollback()
                            log.debug("Transaction rolled back after non-retryable error")
                    except Exception as rollback_err:
                        log.warning(f"Rollback after error failed: {rollback_err}")
                    raise

        raise last_exception

    def execute(self, sql: str, parameters: tuple = None):
        """Execute SQL with automatic retry and placeholder conversion."""
        cursor = self._execute_with_retry("execute", sql, parameters)
        return cursor

    def commit(self):
        """Commit transaction with automatic retry."""
        return self._execute_with_retry("commit")

    def rollback(self):
        """Rollback the current transaction to reset connection state."""
        with self._lock:
            if self._conn:
                try:
                    self._conn.rollback()
                except Exception as e:
                    log.warning(f"Rollback failed: {e}")

    def executemany(self, sql: str, parameters_list: list):
        """Execute SQL with multiple parameter sets (batch operation)."""
        converted_sql = self._convert_sql(sql)
        with self._lock:
            cursor = self._conn.cursor()
            cursor.executemany(converted_sql, parameters_list)
            return cursor

    def fetchone(self):
        """Fetch one result from last execute."""
        with self._lock:
            return self._conn.cursor().fetchone()

    def fetchall(self):
        """Fetch all results from last execute."""
        with self._lock:
            return self._conn.cursor().fetchall()

    def close(self):
        """Close the connection."""
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    def __getattr__(self, name):
        """Delegate other attributes to underlying connection."""
        return getattr(self._conn, name)


# Track current database backend globally
_current_backend = None


def get_database_backend() -> str:
    """Get the current database backend type from config."""
    global _current_backend
    if _current_backend is None:
        _current_backend = get_config().database_backend.lower()
    return _current_backend


def is_postgres() -> bool:
    """Check if using PostgreSQL backend."""
    return get_database_backend() == "postgres"


def get_connection():
    """
    Get a resilient connection to the database.

    Returns a connection wrapper appropriate for the configured backend.

    Configuration can be set via config/channels.yaml settings section:
    - database_backend: "turso" (default) or "postgres"
    - database_url: Connection URL for Turso/libsql

    Or via environment variables (for secrets):
    - TURSO_AUTH_TOKEN: Your Turso auth token (not needed for local)
    - POSTGRES_URL: postgresql://user:pass@host:port/dbname
    """
    cfg = get_config()
    backend = get_database_backend()

    if backend == "postgres":
        # PostgreSQL backend
        conn_string = cfg.postgres_url
        if not conn_string:
            raise ValueError("POSTGRES_URL environment variable required for postgres backend")

        log.debug(f"Connecting to PostgreSQL: {conn_string[:30]}...")
        return PostgresConnection(conn_string)

    else:
        # Turso/libsql backend (default)
        import libsql

        url = cfg.database_url
        auth_token = cfg.database_auth_token

        log.debug(f"Connecting to database: {url[:30]}...")

        if url.startswith("libsql://") or url.startswith("https://"):
            # Remote Turso database
            conn = libsql.connect(database=url, auth_token=auth_token)
        else:
            # Local SQLite file
            # Ensure directory exists for local file
            if url.startswith("file:"):
                filepath = url[5:]
                os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
            conn = libsql.connect(database=url)

        # Wrap in TursoConnection for retry logic and connection refresh capability
        return TursoConnection(conn, url=url, auth_token=auth_token)


def init_database(conn) -> None:
    """Initialize database schema.

    Automatically uses PostgreSQL-specific schema when that backend is active.
    """
    if is_postgres():
        return init_database_postgres(conn)

    # SQLite/Turso schema follows
    # Channels dimension table
    conn.execute("""
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
    
    conn.commit()


def init_database_postgres(conn) -> None:
    """Initialize PostgreSQL database schema.

    Uses PostgreSQL-specific syntax (SERIAL, TIMESTAMP, etc.)
    """
    # Channels dimension table
    conn.execute("""
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
            subscriber_count BIGINT,
            view_count BIGINT,
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
            view_count BIGINT,
            like_count BIGINT,
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

    # Fetch log - use SERIAL for auto-increment in PostgreSQL
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            fetch_id SERIAL PRIMARY KEY,
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

    # Create indexes (PostgreSQL syntax is the same)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_fetched ON video_stats(fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_video ON video_stats(video_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_published ON comments(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_channel_stats_fetched ON channel_stats(fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_published ON videos(channel_id, published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_video_published ON comments(video_id, published_at)")

    conn.commit()


# ============================================================================
# QUOTA TRACKING - Persist across runs
# ============================================================================

def get_quota_usage(conn, date_str: str) -> Optional[dict]:
    """
    Get quota usage for a specific date from database.
    
    Args:
        conn: Database connection
        date_str: Date in ISO format (YYYY-MM-DD)
    
    Returns:
        Dict with 'used' and 'operations' or None if not found
    """
    result = conn.execute("""
        SELECT used, operations FROM quota_usage WHERE date = ?
    """, (date_str,)).fetchone()
    
    if result:
        import json
        return {
            'used': result[0],
            'operations': json.loads(result[1]) if result[1] else {}
        }
    return None


def save_quota_usage(conn, date_str: str, used: int, operations: dict) -> None:
    """
    Save quota usage to database.
    
    Args:
        conn: Database connection
        date_str: Date in ISO format (YYYY-MM-DD)
        used: Total quota units used
        operations: Dict of operation -> units used
    """
    import json
    conn.execute("""
        INSERT INTO quota_usage (date, used, operations, last_updated)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            used = excluded.used,
            operations = excluded.operations,
            last_updated = excluded.last_updated
    """, (date_str, used, json.dumps(operations), datetime.now().isoformat()))
    conn.commit()


# ============================================================================
# SMART INCREMENTAL FETCHING - Minimize redundant requests
# ============================================================================

def get_last_fetch_time(conn, channel_id: str) -> Optional[datetime]:
    """Get the last successful fetch time for a channel."""
    result = conn.execute("""
        SELECT MAX(completed_at) 
        FROM fetch_log 
        WHERE channel_id = ? AND status = 'completed'
    """, (channel_id,)).fetchone()
    
    if result and result[0]:
        return datetime.fromisoformat(result[0])
    return None


def get_latest_video_publish_date(conn, channel_id: str) -> Optional[datetime]:
    """Get the publish date of the most recent video for a channel."""
    result = conn.execute("""
        SELECT MAX(published_at) 
        FROM videos 
        WHERE channel_id = ?
    """, (channel_id,)).fetchone()
    
    if result and result[0]:
        return datetime.fromisoformat(result[0].replace("Z", "+00:00"))
    return None


def get_existing_video_ids(conn, channel_id: str) -> set[str]:
    """Get all video IDs already in database for a channel."""
    result = conn.execute("""
        SELECT video_id FROM videos WHERE channel_id = ?
    """, (channel_id,)).fetchall()
    return {row[0] for row in result}


def get_videos_without_transcripts(conn, channel_id: str) -> list[str]:
    """Get video IDs that don't have transcripts yet."""
    result = conn.execute("""
        SELECT v.video_id 
        FROM videos v
        LEFT JOIN transcripts t ON v.video_id = t.video_id
        WHERE v.channel_id = ? AND t.video_id IS NULL
    """, (channel_id,)).fetchall()
    return [row[0] for row in result]


def get_latest_comment_time(conn, video_id: str) -> Optional[datetime]:
    """Get the most recent comment timestamp for a video."""
    result = conn.execute("""
        SELECT MAX(published_at) FROM comments WHERE video_id = ?
    """, (video_id,)).fetchone()
    
    if result and result[0]:
        return datetime.fromisoformat(result[0].replace('Z', '+00:00'))
    return None


def get_videos_needing_comments(
    conn,
    channel_id: str,
    refresh_tiers: list[dict] = None,
    limit: int = None,
    min_new_comments: int = 0
) -> list[str]:
    """
    Get video IDs that need comment updates.

    Smart detection: Only fetches comments when YouTube's comment_count
    is higher than the number of comments we have stored.

    Uses tiered refresh periods based on video age.
    Optimized to use a single query with CASE statements instead of multiple queries.

    Args:
        channel_id: Channel to check
        refresh_tiers: List of dicts with 'max_age_days' and 'refresh_hours'.
                      Tiers are processed in order, videos match first applicable tier.
                      Default: [
                          {'max_age_days': 2, 'refresh_hours': 6},      # < 48h: every 6h
                          {'max_age_days': 7, 'refresh_hours': 12},     # 48h-7d: every 12h
                          {'max_age_days': 30, 'refresh_hours': 48},    # 7d-30d: every 48h
                          {'max_age_days': None, 'refresh_hours': 168}  # 30d+: every 7 days
                      ]
        limit: Maximum total videos to return (default: None = unlimited)
        min_new_comments: Minimum number of new comments required to fetch (default: 0 = all videos)
                         Set to 10 to skip videos with < 10 new comments since last fetch

    Returns:
        List of video IDs needing comment updates, newest first
    """
    if refresh_tiers is None:
        refresh_tiers = [
            {'max_age_days': 2, 'refresh_hours': 6},      # < 48h: every 6h
            {'max_age_days': 7, 'refresh_hours': 12},     # 48h-7d: every 12h
            {'max_age_days': 30, 'refresh_hours': 48},    # 7d-30d: every 48h
            {'max_age_days': None, 'refresh_hours': 168}  # 30d+: every 7 days
        ]

    # Build CASE statement for refresh hours based on video age
    case_parts = []
    for i, tier in enumerate(refresh_tiers):
        max_age = tier.get('max_age_days')
        refresh_hours = tier.get('refresh_hours', 24)

        if i == 0:
            min_age = 0
        else:
            min_age = refresh_tiers[i - 1].get('max_age_days', 0)

        if max_age is None:
            # Last tier
            case_parts.append(f"WHEN datetime(v.published_at) < datetime('now', '-{min_age} days') THEN {refresh_hours}")
        elif min_age == 0:
            # First tier
            case_parts.append(f"WHEN datetime(v.published_at) >= datetime('now', '-{max_age} days') THEN {refresh_hours}")
        else:
            # Middle tier
            case_parts.append(
                f"WHEN datetime(v.published_at) < datetime('now', '-{min_age} days') "
                f"AND datetime(v.published_at) >= datetime('now', '-{max_age} days') THEN {refresh_hours}"
            )

    refresh_hours_case = "CASE " + " ".join(case_parts) + " ELSE 168 END"

    # Single optimized query that handles all tiers at once
    query = f"""
        SELECT v.video_id, v.published_at,
               COALESCE(vs.comment_count, 0) as youtube_count,
               COALESCE(stored.stored_count, 0) as stored_count,
               ({refresh_hours_case}) as tier_refresh_hours
        FROM videos v
        LEFT JOIN (
            SELECT video_id, comment_count,
                   ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY fetched_at DESC) as rn
            FROM video_stats
        ) vs ON v.video_id = vs.video_id AND vs.rn = 1
        LEFT JOIN (
            SELECT video_id, COUNT(*) as stored_count, MAX(fetched_at) as last_fetch
            FROM comments
            GROUP BY video_id
        ) stored ON v.video_id = stored.video_id
        WHERE v.channel_id = ?
        AND COALESCE(vs.comment_count, 0) > COALESCE(stored.stored_count, 0)
        AND (COALESCE(vs.comment_count, 0) - COALESCE(stored.stored_count, 0)) >= ?
        AND (
            stored.last_fetch IS NULL
            OR datetime(stored.last_fetch) < datetime('now', '-' || ({refresh_hours_case}) || ' hours')
        )
        ORDER BY v.published_at DESC
    """
    if limit is not None:
        query += f" LIMIT {limit}"

    log.debug(f"Querying videos needing comments for channel {channel_id}")
    start_time = time.time()
    videos = conn.execute(query, (channel_id, min_new_comments)).fetchall()
    elapsed = time.time() - start_time

    if elapsed > 5.0:
        log.warning(f"Slow query in get_videos_needing_comments: {elapsed:.1f}s for channel {channel_id}")
    else:
        log.debug(f"Query completed in {elapsed:.1f}s")

    video_ids = [row[0] for row in videos]

    # Log tier breakdown for debugging
    tier_counts = {}
    for row in videos:
        refresh_h = row[4]
        tier_counts[refresh_h] = tier_counts.get(refresh_h, 0) + 1

    for tier in refresh_tiers:
        max_age = tier.get('max_age_days')
        refresh_hours = tier.get('refresh_hours', 24)
        count = tier_counts.get(refresh_hours, 0)
        tier_desc = f"<{max_age}d" if max_age else ">30d"
        log.debug(f"Comments tier {tier_desc} (refresh {refresh_hours}h): {count} videos")

    log.debug(f"Total videos needing comments: {len(video_ids)}")
    return video_ids


def get_videos_needing_stats_update(
    conn,
    channel_id: str,
    refresh_tiers: list[dict] = None,
    limit: int = None
) -> list[str]:
    """
    Get video IDs that need stats refresh.

    Uses tiered refresh periods based on video age.
    Optimized to use a single query with CASE statements instead of multiple queries.

    Args:
        channel_id: Channel to check
        refresh_tiers: List of dicts with 'max_age_days' and 'refresh_hours'.
                      Default: [
                          {'max_age_days': 2, 'refresh_hours': 0},      # < 48h: every run
                          {'max_age_days': 7, 'refresh_hours': 6},      # 48h-7d: every 6h
                          {'max_age_days': 30, 'refresh_hours': 12},    # 7d-30d: every 12h
                          {'max_age_days': None, 'refresh_hours': 24}   # 30d+: every 24h
                      ]
        limit: Maximum total videos to return (default: None = unlimited)

    Returns:
        List of video IDs needing stats update, newest first
    """
    if refresh_tiers is None:
        refresh_tiers = [
            {'max_age_days': 2, 'refresh_hours': 0},      # < 48h: every run
            {'max_age_days': 7, 'refresh_hours': 6},      # 48h-7d: every 6h
            {'max_age_days': 30, 'refresh_hours': 12},    # 7d-30d: every 12h
            {'max_age_days': None, 'refresh_hours': 24}   # 30d+: every 24h
        ]

    # Build CASE statement for refresh hours based on video age
    case_parts = []
    for i, tier in enumerate(refresh_tiers):
        max_age = tier.get('max_age_days')
        refresh_hours = tier.get('refresh_hours', 24)

        if i == 0:
            min_age = 0
        else:
            min_age = refresh_tiers[i - 1].get('max_age_days', 0)

        if max_age is None:
            # Last tier
            case_parts.append(f"WHEN datetime(v.published_at) < datetime('now', '-{min_age} days') THEN {refresh_hours}")
        elif min_age == 0:
            # First tier
            case_parts.append(f"WHEN datetime(v.published_at) >= datetime('now', '-{max_age} days') THEN {refresh_hours}")
        else:
            # Middle tier
            case_parts.append(
                f"WHEN datetime(v.published_at) < datetime('now', '-{min_age} days') "
                f"AND datetime(v.published_at) >= datetime('now', '-{max_age} days') THEN {refresh_hours}"
            )

    refresh_hours_case = "CASE " + " ".join(case_parts) + " ELSE 24 END"

    # Single optimized query that handles all tiers at once
    query = f"""
        SELECT v.video_id, v.published_at,
               ({refresh_hours_case}) as tier_refresh_hours
        FROM videos v
        LEFT JOIN (
            SELECT video_id, MAX(fetched_at) as last_fetch
            FROM video_stats
            GROUP BY video_id
        ) vs ON v.video_id = vs.video_id
        WHERE v.channel_id = ?
        AND (
            vs.last_fetch IS NULL
            OR datetime(vs.last_fetch) < datetime('now', '-' || ({refresh_hours_case}) || ' hours')
        )
        ORDER BY v.published_at DESC
    """
    if limit is not None:
        query += f" LIMIT {limit}"

    log.debug(f"Querying videos needing stats for channel {channel_id}")
    start_time = time.time()
    videos = conn.execute(query, (channel_id,)).fetchall()
    elapsed = time.time() - start_time

    if elapsed > 5.0:
        log.warning(f"Slow query in get_videos_needing_stats_update: {elapsed:.1f}s for channel {channel_id}")
    else:
        log.debug(f"Query completed in {elapsed:.1f}s")

    video_ids = [row[0] for row in videos]

    # Log tier breakdown for debugging
    tier_counts = {}
    for row in videos:
        refresh_h = row[2]
        tier_counts[refresh_h] = tier_counts.get(refresh_h, 0) + 1

    for tier in refresh_tiers:
        max_age = tier.get('max_age_days')
        refresh_hours = tier.get('refresh_hours', 24)
        count = tier_counts.get(refresh_hours, 0)
        tier_desc = f"<{max_age}d" if max_age else ">30d"
        log.debug(f"Stats tier {tier_desc} (refresh {refresh_hours}h): {count} videos")

    log.debug(f"Total videos needing stats update: {len(video_ids)}")
    return video_ids


def _parse_datetime_utc(dt_string: str) -> datetime:
    """
    Parse a datetime string to a timezone-aware UTC datetime.

    Handles both timezone-naive and timezone-aware strings from the database.
    This prevents TypeError when comparing with datetime.now(timezone.utc).

    Args:
        dt_string: ISO format datetime string (may or may not have timezone)

    Returns:
        Timezone-aware datetime in UTC
    """
    dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        # Assume UTC for naive datetimes from database
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def should_update_playlists(conn, channel_id: str, hours: int = 24) -> bool:
    """Check if playlists should be updated (not updated in last N hours)."""
    result = conn.execute("""
        SELECT MAX(updated_at) FROM playlists WHERE channel_id = ?
    """, (channel_id,)).fetchone()

    if not result or not result[0]:
        return True

    last_update = _parse_datetime_utc(result[0])
    hours_ago = datetime.now(timezone.utc) - last_update
    return hours_ago.total_seconds() > (hours * 3600)


def should_update_channel_stats(conn, channel_id: str, hours: int = 6) -> bool:
    """Check if channel stats should be updated (not updated in last N hours)."""
    result = conn.execute("""
        SELECT MAX(fetched_at) FROM channel_stats WHERE channel_id = ?
    """, (channel_id,)).fetchone()
    
    if not result or not result[0]:
        return True
    
    last_fetch = _parse_datetime_utc(result[0])
    hours_ago = datetime.now(timezone.utc) - last_fetch
    return hours_ago.total_seconds() > (hours * 3600)


# ============================================================================
# WRITE OPERATIONS
# ============================================================================

def start_fetch_log(conn, channel_id: str, fetch_type: str) -> int:
    """Start a fetch log entry, return the fetch_id."""
    now = datetime.now().isoformat()

    if is_postgres():
        # PostgreSQL: use RETURNING to get the auto-generated ID
        result = conn.execute("""
            INSERT INTO fetch_log (channel_id, fetch_type, started_at, status)
            VALUES (?, ?, ?, 'running')
            RETURNING fetch_id
        """, (channel_id, fetch_type, now,))
        row = result.fetchone()
        conn.commit()
        return row[0]
    else:
        # SQLite/Turso: use last_insert_rowid()
        conn.execute("""
            INSERT INTO fetch_log (channel_id, fetch_type, started_at, status)
            VALUES (?, ?, ?, 'running')
        """, (channel_id, fetch_type, now,))
        conn.commit()
        result = conn.execute("SELECT last_insert_rowid()").fetchone()
        return result[0]


def complete_fetch_log(conn, fetch_id: int, 
                       videos: int, comments: int, transcripts: int, 
                       status: str = 'completed', errors: str = None) -> None:
    """Complete a fetch log entry."""
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE fetch_log 
        SET completed_at = ?,
            videos_fetched = ?,
            comments_fetched = ?,
            transcripts_fetched = ?,
            status = ?,
            errors = ?
        WHERE fetch_id = ?
    """, (now, videos, comments, transcripts, status, errors, fetch_id,))
    conn.commit()


def upsert_channel(conn, channel: dict) -> None:
    """Insert or update channel metadata."""
    now = datetime.now().isoformat()
    
    # Convert list to JSON string
    topic_categories = json.dumps(channel.get('topic_categories', []))
    
    conn.execute("""
        INSERT INTO channels (
            channel_id, title, description, custom_url, country, published_at,
            thumbnail_url, banner_url, keywords, topic_categories, 
            uploads_playlist_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            title = excluded.title,
            description = excluded.description,
            custom_url = excluded.custom_url,
            thumbnail_url = excluded.thumbnail_url,
            banner_url = excluded.banner_url,
            keywords = excluded.keywords,
            topic_categories = excluded.topic_categories,
            updated_at = excluded.updated_at
    """, (
        channel['channel_id'],
        channel.get('title'),
        channel.get('description'),
        channel.get('custom_url'),
        channel.get('country'),
        channel.get('published_at'),
        channel.get('thumbnail_url'),
        channel.get('banner_url'),
        channel.get('keywords'),
        topic_categories,
        channel.get('uploads_playlist_id'),
        now
    ))
    conn.commit()


def insert_channel_stats(conn, channel_id: str, stats: dict) -> None:
    """Insert channel stats snapshot."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT OR IGNORE INTO channel_stats (channel_id, fetched_at, subscriber_count, view_count, video_count)
        VALUES (?, ?, ?, ?, ?)
    """, (
        channel_id,
        now,
        stats.get('subscriber_count', 0),
        stats.get('view_count', 0),
        stats.get('video_count', 0)
    ,))
    conn.commit()


def upsert_video(conn, video: dict, commit: bool = True) -> None:
    """Insert or update video metadata."""
    now = datetime.now().isoformat()
    
    # Convert lists to JSON strings
    tags = json.dumps(video.get('tags', []))
    topic_categories = json.dumps(video.get('topic_categories', []))
    
    conn.execute("""
        INSERT INTO videos (
            video_id, channel_id, title, description, published_at,
            duration_seconds, duration_iso, category_id, default_language,
            default_audio_language, tags, thumbnail_url, caption_available,
            definition, dimension, projection, privacy_status, license,
            embeddable, made_for_kids, topic_categories, has_chapters, 
            first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            title = excluded.title,
            description = excluded.description,
            tags = excluded.tags,
            thumbnail_url = excluded.thumbnail_url,
            caption_available = excluded.caption_available,
            privacy_status = excluded.privacy_status,
            has_chapters = excluded.has_chapters,
            updated_at = excluded.updated_at
    """, (
        video['video_id'],
        video.get('channel_id'),
        video.get('title'),
        video.get('description'),
        video.get('published_at'),
        video.get('duration_seconds'),
        video.get('duration_iso'),
        video.get('category_id'),
        video.get('default_language'),
        video.get('default_audio_language'),
        tags,
        video.get('thumbnail_url'),
        1 if video.get('caption_available') else 0,
        video.get('definition'),
        video.get('dimension'),
        video.get('projection'),
        video.get('privacy_status'),
        video.get('license'),
        1 if video.get('embeddable') else 0,
        1 if video.get('made_for_kids') else 0,
        topic_categories,
        1 if video.get('has_chapters') else 0,
        now,  # first_seen_at (will be ignored on conflict)
        now   # updated_at
    ))
    if commit:
        conn.commit()


def insert_video_stats(conn, video_id: str, stats: dict, commit: bool = True) -> None:
    """Insert video stats snapshot."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT OR IGNORE INTO video_stats (video_id, fetched_at, view_count, like_count, comment_count)
        VALUES (?, ?, ?, ?, ?)
    """, (
        video_id,
        now,
        stats.get('view_count', 0),
        stats.get('like_count', 0),
        stats.get('comment_count', 0)
    ))
    if commit:
        conn.commit()


def insert_video_stats_batch(conn, video_stats: list[tuple[str, dict]]) -> int:
    """Insert multiple video stats in a batch operation.

    Uses executemany with the TursoConnection wrapper for reliable retry logic.

    Args:
        conn: Database connection with retry support
        video_stats: List of (video_id, stats_dict) tuples

    Returns:
        Number of stats inserted
    """
    if not video_stats:
        return 0

    now = datetime.now().isoformat()

    # Prepare parameters for batch insert
    params = [
        (
            video_id,
            now,
            stats.get('view_count', 0),
            stats.get('like_count', 0),
            stats.get('comment_count', 0)
        )
        for video_id, stats in video_stats
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO video_stats (video_id, fetched_at, view_count, like_count, comment_count) VALUES (?, ?, ?, ?, ?)",
        params
    )
    conn.commit()

    return len(video_stats)


def upsert_videos_batch(conn, videos: list[dict], commit: bool = True) -> int:
    """Insert or update multiple videos in a single batch operation.

    Args:
        conn: Database connection
        videos: List of video dicts with video metadata

    Returns:
        Number of videos upserted
    """
    if not videos:
        return 0

    now = datetime.now().isoformat()

    # Prepare all parameters at once
    params = []
    for video in videos:
        tags = json.dumps(video.get('tags', []))
        topic_categories = json.dumps(video.get('topic_categories', []))
        params.append((
            video['video_id'],
            video.get('channel_id'),
            video.get('title'),
            video.get('description'),
            video.get('published_at'),
            video.get('duration_seconds'),
            video.get('duration_iso'),
            video.get('category_id'),
            video.get('default_language'),
            video.get('default_audio_language'),
            tags,
            video.get('thumbnail_url'),
            1 if video.get('caption_available') else 0,
            video.get('definition'),
            video.get('dimension'),
            video.get('projection'),
            video.get('privacy_status'),
            video.get('license'),
            1 if video.get('embeddable') else 0,
            1 if video.get('made_for_kids') else 0,
            topic_categories,
            1 if video.get('has_chapters') else 0,
            now,  # first_seen_at
            now   # updated_at
        ))

    # Single batch upsert
    conn.executemany("""
        INSERT INTO videos (
            video_id, channel_id, title, description, published_at,
            duration_seconds, duration_iso, category_id, default_language,
            default_audio_language, tags, thumbnail_url, caption_available,
            definition, dimension, projection, privacy_status, license,
            embeddable, made_for_kids, topic_categories, has_chapters,
            first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            title = excluded.title,
            description = excluded.description,
            tags = excluded.tags,
            thumbnail_url = excluded.thumbnail_url,
            caption_available = excluded.caption_available,
            privacy_status = excluded.privacy_status,
            has_chapters = excluded.has_chapters,
            updated_at = excluded.updated_at
    """, params)

    if commit:
        conn.commit()
    return len(videos)


def upsert_chapters_batch(conn, chapters_by_video: dict[str, list[dict]], commit: bool = True) -> int:
    """Replace chapters for multiple videos in batch.

    Args:
        conn: Database connection
        chapters_by_video: Dict mapping video_id to list of chapter dicts

    Returns:
        Total number of chapters inserted
    """
    if not chapters_by_video:
        return 0

    # Delete old chapters for all videos in one query
    video_ids = list(chapters_by_video.keys())
    placeholders = ','.join(['?' for _ in video_ids])
    conn.execute(f"DELETE FROM chapters WHERE video_id IN ({placeholders})", tuple(video_ids))

    # Prepare all chapter inserts
    params = []
    for video_id, chapters in chapters_by_video.items():
        for i, chapter in enumerate(chapters):
            params.append((
                video_id,
                i,
                chapter.get('title'),
                chapter.get('start_seconds'),
                chapter.get('end_seconds')
            ))

    if params:
        conn.executemany("""
            INSERT INTO chapters (video_id, chapter_index, title, start_seconds, end_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, params)

    if commit:
        conn.commit()
    return len(params)


def upsert_chapters(conn, video_id: str, chapters: list[dict], commit: bool = True) -> None:
    """Replace chapters for a video."""
    conn.execute("DELETE FROM chapters WHERE video_id = ?", (video_id,))
    for i, chapter in enumerate(chapters):
        conn.execute("""
            INSERT INTO chapters (video_id, chapter_index, title, start_seconds, end_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (
            video_id,
            i,
            chapter.get('title'),
            chapter.get('start_seconds'),
            chapter.get('end_seconds')
        ,))
    if commit:
        conn.commit()


def insert_transcript(conn, video_id: str, transcript: dict) -> bool:
    """
    Insert transcript (only if not exists).
    Returns True if inserted, False if already exists.
    """
    # Check if already exists
    existing = conn.execute(
        "SELECT 1 FROM transcripts WHERE video_id = ?", (video_id,)
    ).fetchone()
    
    if existing:
        return False
    
    now = datetime.now().isoformat()
    entries_json = json.dumps(transcript.get('entries', []))
    
    conn.execute("""
        INSERT INTO transcripts (video_id, language, language_code, transcript_type, full_text, entries_json, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        video_id,
        transcript.get('language'),
        transcript.get('language_code'),
        transcript.get('transcript_type'),
        transcript.get('full_text'),
        entries_json,
        now
    ))
    conn.commit()
    return True


def insert_comments(conn, comments: list[dict]) -> int:
    """Insert comments, skip duplicates. Returns count of new comments."""
    if not comments:
        return 0

    now = datetime.now().isoformat()

    # Batch insert with INSERT OR IGNORE (skips duplicates via PRIMARY KEY)
    # Use cursor.rowcount to get inserted count without expensive table scans
    cursor = conn.executemany("""
        INSERT OR IGNORE INTO comments (
            comment_id, video_id, parent_comment_id, author_display_name,
            author_channel_id, text, like_count, published_at, updated_at, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (c['comment_id'], c['video_id'], c.get('parent_comment_id'),
         c.get('author_display_name'), c.get('author_channel_id'),
         c.get('text'), c.get('like_count', 0), c.get('published_at'),
         c.get('updated_at'), now)
        for c in comments
    ])

    conn.commit()

    # rowcount returns number of rows actually inserted (ignored duplicates not counted)
    return cursor.rowcount


def upsert_playlist(conn, playlist: dict) -> None:
    """Insert or update playlist."""
    now = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO playlists (
            playlist_id, channel_id, title, description, published_at,
            thumbnail_url, item_count, privacy_status, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(playlist_id) DO UPDATE SET
            title = excluded.title,
            description = excluded.description,
            thumbnail_url = excluded.thumbnail_url,
            item_count = excluded.item_count,
            privacy_status = excluded.privacy_status,
            updated_at = excluded.updated_at
    """, (
        playlist['playlist_id'],
        playlist.get('channel_id'),
        playlist.get('title'),
        playlist.get('description'),
        playlist.get('published_at'),
        playlist.get('thumbnail_url'),
        playlist.get('item_count'),
        playlist.get('privacy_status'),
        now
    ))
    conn.commit()


# Allowed tables for export (security: prevents SQL injection via table names)
ALLOWED_EXPORT_TABLES = frozenset([
    'channels', 'channel_stats', 'videos', 'video_stats',
    'chapters', 'transcripts', 'comments', 'playlists'
])


def export_to_csv(conn, output_dir: str = "exports") -> dict[str, str]:
    """
    Export tables to CSV files.

    Only exports from a predefined allowlist of tables for security.
    """
    import csv
    os.makedirs(output_dir, exist_ok=True)

    exported = {}
    for table in ALLOWED_EXPORT_TABLES:
        # Security: Validate table name is in allowlist (defensive check)
        if table not in ALLOWED_EXPORT_TABLES:
            log.warning(f"Skipping unauthorized table: {table}")
            continue

        output_path = f"{output_dir}/{table}.csv"
        # Table name is validated above, safe to use in query
        result = conn.execute(f"SELECT * FROM {table}").fetchall()

        if result:
            # Get column names
            cursor = conn.execute(f"SELECT * FROM {table} LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(result)

        exported[table] = output_path

    return exported


# ============================================================================
# PROGRESS TRACKING FOR RESUMABLE OPERATIONS
# ============================================================================

def get_progress(conn, channel_id: str, fetch_id: int, operation: str) -> Optional[dict]:
    """
    Get progress for a resumable operation.
    
    Returns:
        Dict with 'processed_ids' (set) and 'total_count', or None if no progress.
    """
    result = conn.execute("""
        SELECT processed_ids, total_count FROM fetch_progress
        WHERE channel_id = ? AND fetch_id = ? AND operation = ?
    """, (channel_id, fetch_id, operation,)).fetchone()
    
    if result:
        processed_ids = set(json.loads(result[0])) if result[0] else set()
        return {
            'processed_ids': processed_ids,
            'total_count': result[1]
        }
    return None


# Helper function for checkpoint slow threshold from config
def get_checkpoint_slow_threshold_ms() -> int:
    """Get checkpoint slow threshold from config."""
    return get_config().checkpoint_slow_threshold_ms


def save_progress(conn, channel_id: str, fetch_id: int, operation: str,
                  processed_ids: set, total_count: int) -> None:
    """Save progress for a resumable operation."""
    now = datetime.now().isoformat()

    # Monitor JSON serialization performance for large ID sets
    start = time.perf_counter()
    processed_json = json.dumps(list(processed_ids))
    serialize_ms = (time.perf_counter() - start) * 1000

    if serialize_ms > get_checkpoint_slow_threshold_ms():
        log.warning(
            f"Slow checkpoint serialization: {len(processed_ids)} IDs took "
            f"{serialize_ms:.1f}ms for {operation} on channel {channel_id}"
        )

    conn.execute("""
        INSERT INTO fetch_progress (channel_id, fetch_id, operation, processed_ids, total_count, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id, fetch_id, operation) DO UPDATE SET
            processed_ids = excluded.processed_ids,
            total_count = excluded.total_count,
            last_updated = excluded.last_updated
    """, (channel_id, fetch_id, operation, processed_json, total_count, now,))
    conn.commit()


def clear_progress(conn, channel_id: str, fetch_id: int, operation: str = None) -> None:
    """Clear progress for an operation or all operations for a fetch."""
    if operation:
        conn.execute("""
            DELETE FROM fetch_progress 
            WHERE channel_id = ? AND fetch_id = ? AND operation = ?
        """, (channel_id, fetch_id, operation,))
    else:
        conn.execute("""
            DELETE FROM fetch_progress 
            WHERE channel_id = ? AND fetch_id = ?
        """, (channel_id, fetch_id,))
    conn.commit()


# ============================================================================
# VIDEO DELETION DETECTION & CLEANUP
# ============================================================================

def get_all_video_ids_for_channel(conn, channel_id: str) -> set[str]:
    """Get all video IDs we have stored for a channel."""
    result = conn.execute("""
        SELECT video_id FROM videos WHERE channel_id = ?
    """, (channel_id,)).fetchall()
    return {row[0] for row in result}


def mark_videos_as_deleted(conn, video_ids: list[str]) -> int:
    """
    Mark videos as deleted (set privacy_status to 'deleted').
    Returns count of videos marked.
    """
    if not video_ids:
        return 0
    
    now = datetime.now().isoformat()
    count = 0
    
    for video_id in video_ids:
        conn.execute("""
            UPDATE videos 
            SET privacy_status = 'deleted', updated_at = ?
            WHERE video_id = ?
        """, (now, video_id,))
        count += 1
    
    conn.commit()
    return count


def get_deleted_videos(conn, channel_id: str = None) -> list[dict]:
    """Get videos marked as deleted."""
    if channel_id:
        result = conn.execute("""
            SELECT video_id, title, channel_id, updated_at 
            FROM videos 
            WHERE privacy_status = 'deleted' AND channel_id = ?
        """, (channel_id,)).fetchall()
    else:
        result = conn.execute("""
            SELECT video_id, title, channel_id, updated_at 
            FROM videos 
            WHERE privacy_status = 'deleted'
        """).fetchall()
    
    return [{"video_id": r[0], "title": r[1], "channel_id": r[2], "updated_at": r[3]} for r in result]


def purge_deleted_videos(conn, channel_id: str = None, older_than_days: int = 30) -> int:
    """
    Permanently remove videos marked as deleted for more than N days.
    Returns count of videos purged.
    """
    cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
    
    if channel_id:
        # Get video IDs to delete
        result = conn.execute("""
            SELECT video_id FROM videos 
            WHERE privacy_status = 'deleted' 
            AND channel_id = ? 
            AND updated_at < ?
        """, (channel_id, cutoff,)).fetchall()
    else:
        result = conn.execute("""
            SELECT video_id FROM videos 
            WHERE privacy_status = 'deleted' 
            AND updated_at < ?
        """, (cutoff,)).fetchall()
    
    video_ids = [r[0] for r in result]
    
    if not video_ids:
        return 0
    
    # Delete from all related tables
    for video_id in video_ids:
        conn.execute("DELETE FROM video_stats WHERE video_id = ?", (video_id,))
        conn.execute("DELETE FROM chapters WHERE video_id = ?", (video_id,))
        conn.execute("DELETE FROM transcripts WHERE video_id = ?", (video_id,))
        conn.execute("DELETE FROM comments WHERE video_id = ?", (video_id,))
        conn.execute("DELETE FROM videos WHERE video_id = ?", (video_id,))
    
    conn.commit()
    return len(video_ids)
