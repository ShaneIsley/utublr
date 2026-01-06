"""
Database schema and operations for YouTube metadata tracking.
Uses Turso (libSQL) for persistent cloud storage.

Supports:
- Remote Turso database (recommended for production)
- Local SQLite file (for development/testing)

Features:
- Automatic retry with exponential backoff for transient errors
- Connection wrapper for resilient database operations
"""

import json
import os
import time
from datetime import datetime
from functools import wraps
from typing import Optional, Callable, Any

# Try to import logger, fall back to print if not available
try:
    from logger import get_logger
    log = get_logger("database")
except ImportError:
    import logging
    log = logging.getLogger("database")


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
]

# Retry configuration optimized for Turso free tier
DB_MAX_RETRIES = 5
DB_BASE_DELAY = 1.0  # Start with 1 second
DB_MAX_DELAY = 30.0  # Cap at 30 seconds
DB_EXPONENTIAL_BASE = 2.0


def is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable based on known patterns."""
    error_str = str(error).lower()
    for pattern in RETRYABLE_ERROR_PATTERNS:
        if pattern.lower() in error_str:
            return True
    return False


def retry_db_operation(
    max_retries: int = DB_MAX_RETRIES,
    base_delay: float = DB_BASE_DELAY,
    max_delay: float = DB_MAX_DELAY,
):
    """
    Decorator for retrying database operations with exponential backoff.
    
    Handles Turso-specific transient errors like 502, 503, connection issues.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if is_retryable_error(e):
                        last_exception = e
                        if attempt < max_retries:
                            delay = min(base_delay * (DB_EXPONENTIAL_BASE ** attempt), max_delay)
                            log.warning(f"Database error (attempt {attempt + 1}/{max_retries + 1}), "
                                       f"retrying in {delay:.1f}s: {e}")
                            time.sleep(delay)
                            continue
                        else:
                            log.error(f"Database operation failed after {max_retries + 1} attempts: {e}")
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
    transient Turso errors with exponential backoff.
    """
    
    def __init__(self, conn):
        self._conn = conn
        self._in_transaction = False
    
    @retry_db_operation()
    def execute(self, sql: str, parameters: tuple = None):
        """Execute SQL with automatic retry on transient errors."""
        if parameters:
            return self._conn.execute(sql, parameters)
        return self._conn.execute(sql)
    
    @retry_db_operation()
    def commit(self):
        """Commit transaction with automatic retry."""
        return self._conn.commit()
    
    def __getattr__(self, name):
        """Delegate other attributes to underlying connection."""
        return getattr(self._conn, name)


def get_connection():
    """
    Get a resilient connection to the database.
    
    Returns a TursoConnection wrapper that handles transient errors.
    
    Environment variables:
    - TURSO_DATABASE_URL: libsql://your-db.turso.io (or file:local.db for local)
    - TURSO_AUTH_TOKEN: Your Turso auth token (not needed for local)
    """
    import libsql_experimental as libsql
    
    url = os.environ.get("TURSO_DATABASE_URL", "file:data/youtube.db")
    auth_token = os.environ.get("TURSO_AUTH_TOKEN", "")
    
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
    
    # Wrap in TursoConnection for retry logic
    return TursoConnection(conn)


def init_database(conn) -> None:
    """Initialize database schema."""
    
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


def get_videos_needing_stats_update(conn, channel_id: str, hours_since_last: int = 6) -> list[str]:
    """
    Get video IDs that need stats updated.
    Only returns videos that haven't been updated in the last N hours.
    """
    result = conn.execute("""
        SELECT v.video_id 
        FROM videos v
        LEFT JOIN (
            SELECT video_id, MAX(fetched_at) as last_fetch
            FROM video_stats
            GROUP BY video_id
        ) vs ON v.video_id = vs.video_id
        WHERE v.channel_id = ?
        AND (vs.last_fetch IS NULL 
             OR datetime(vs.last_fetch) < datetime('now', '-' || ? || ' hours'))
    """, (channel_id, hours_since_last,)).fetchall()
    return [row[0] for row in result]


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
    hours_since_last: int = 24,
    hours_since_last_new: int = 6,
    new_video_days: int = 7,
    limit: int = None
) -> list[str]:
    """
    Get video IDs that need comment updates.
    
    Smart detection: Only fetches comments when YouTube's comment_count 
    is higher than the number of comments we have stored.
    
    Uses tiered update frequency for videos we haven't checked recently:
    - New videos (< new_video_days old): check every hours_since_last_new hours
    - Older videos: check every hours_since_last hours
    
    Args:
        channel_id: Channel to check
        hours_since_last: Hours before re-checking comments for older videos (default: 24)
        hours_since_last_new: Hours before re-checking for new videos (default: 6)
        new_video_days: Videos younger than this many days are "new" (default: 7)
        limit: Maximum total videos to return (default: None = unlimited)
    
    Returns:
        List of video IDs needing comment updates, newest first
    """
    # Get new videos (published within new_video_days) that have more comments than we've stored
    # Compare YouTube's reported comment_count (from video_stats) vs our actual stored comments
    new_videos = conn.execute("""
        SELECT v.video_id, 
               COALESCE(vs.comment_count, 0) as youtube_count,
               COALESCE(stored.stored_count, 0) as stored_count
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
        AND v.published_at >= datetime('now', '-' || ? || ' days')
        AND (
            -- Never fetched comments, or
            stored.last_fetch IS NULL 
            -- Haven't checked recently AND YouTube shows more comments than we have
            OR (
                datetime(stored.last_fetch) < datetime('now', '-' || ? || ' hours')
                AND COALESCE(vs.comment_count, 0) > COALESCE(stored.stored_count, 0)
            )
        )
        ORDER BY v.published_at DESC
    """, (channel_id, new_video_days, hours_since_last_new)).fetchall()
    
    new_ids = [row[0] for row in new_videos]
    
    # Log details for debugging
    if new_videos:
        sample = new_videos[:3]
        for vid, yt_count, stored in sample:
            log.debug(f"New video {vid}: YouTube says {yt_count} comments, we have {stored}")
    
    # Calculate how many older videos we can fetch
    older_limit = None
    if limit is not None:
        older_limit = max(0, limit - len(new_ids))
        if older_limit == 0:
            log.debug(f"Videos needing comments: {len(new_ids)} new (<{new_video_days}d), "
                      f"limit reached, skipping older videos")
            return new_ids[:limit]
    
    # Get older videos needing updates (less frequent)
    older_query = """
        SELECT v.video_id,
               COALESCE(vs.comment_count, 0) as youtube_count,
               COALESCE(stored.stored_count, 0) as stored_count
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
        AND v.published_at < datetime('now', '-' || ? || ' days')
        AND (
            -- Never fetched comments, or
            stored.last_fetch IS NULL 
            -- Haven't checked recently AND YouTube shows more comments than we have
            OR (
                datetime(stored.last_fetch) < datetime('now', '-' || ? || ' hours')
                AND COALESCE(vs.comment_count, 0) > COALESCE(stored.stored_count, 0)
            )
        )
        ORDER BY v.published_at DESC
    """
    if older_limit is not None:
        older_query += f" LIMIT {older_limit}"
    
    older_videos = conn.execute(older_query, (channel_id, new_video_days, hours_since_last)).fetchall()
    older_ids = [row[0] for row in older_videos]
    
    log.debug(f"Videos needing comments: {len(new_ids)} new (<{new_video_days}d), "
              f"{len(older_ids)} older (>{new_video_days}d)")
    
    # Return new videos first (they're more active), then older
    return new_ids + older_ids


def should_update_channel_stats(conn, channel_id: str, hours: int = 6) -> bool:
    """Check if channel stats should be updated (not updated in last N hours)."""
    result = conn.execute("""
        SELECT MAX(fetched_at) FROM channel_stats WHERE channel_id = ?
    """, (channel_id,)).fetchone()
    
    if not result or not result[0]:
        return True
    
    last_fetch = datetime.fromisoformat(result[0])
    hours_ago = datetime.now() - last_fetch
    return hours_ago.total_seconds() > (hours * 3600)


# ============================================================================
# WRITE OPERATIONS
# ============================================================================

def start_fetch_log(conn, channel_id: str, fetch_type: str) -> int:
    """Start a fetch log entry, return the fetch_id."""
    now = datetime.now().isoformat()
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
    """Insert multiple video stats in a single transaction.
    
    Args:
        conn: Database connection
        video_stats: List of (video_id, stats_dict) tuples
        
    Returns:
        Number of stats inserted
    """
    now = datetime.now().isoformat()
    count = 0
    for video_id, stats in video_stats:
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
        count += 1
    conn.commit()
    return count


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
    now = datetime.now().isoformat()
    new_count = 0
    
    for comment in comments:
        # Check if exists
        existing = conn.execute(
            "SELECT 1 FROM comments WHERE comment_id = ?", (comment['comment_id'],)
        ).fetchone()
        
        if not existing:
            conn.execute("""
                INSERT INTO comments (
                    comment_id, video_id, parent_comment_id, author_display_name,
                    author_channel_id, text, like_count, published_at, updated_at, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comment['comment_id'],
                comment['video_id'],
                comment.get('parent_comment_id'),
                comment.get('author_display_name'),
                comment.get('author_channel_id'),
                comment.get('text'),
                comment.get('like_count', 0),
                comment.get('published_at'),
                comment.get('updated_at'),
                now
            ))
            new_count += 1
    
    conn.commit()
    return new_count


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


def export_to_csv(conn, output_dir: str = "exports") -> dict[str, str]:
    """Export tables to CSV files."""
    import csv
    os.makedirs(output_dir, exist_ok=True)
    
    tables = ['channels', 'channel_stats', 'videos', 'video_stats', 
              'chapters', 'transcripts', 'comments', 'playlists']
    
    exported = {}
    for table in tables:
        output_path = f"{output_dir}/{table}.csv"
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


def save_progress(conn, channel_id: str, fetch_id: int, operation: str, 
                  processed_ids: set, total_count: int) -> None:
    """Save progress for a resumable operation."""
    now = datetime.now().isoformat()
    processed_json = json.dumps(list(processed_ids))
    
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


# Need timedelta for purge function
from datetime import timedelta
