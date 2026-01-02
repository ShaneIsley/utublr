"""
Database schema and operations for YouTube metadata tracking.
Uses DuckDB for efficient single-file storage with SQL query support.
"""

import duckdb
from pathlib import Path
from datetime import datetime
from typing import Optional


def get_connection(db_path: str = "data/youtube.duckdb") -> duckdb.DuckDBPyConnection:
    """Get a connection to the DuckDB database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)


def init_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize database schema."""
    
    conn.execute("""
        -- Channels dimension table (upserted on each fetch)
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            description VARCHAR,
            custom_url VARCHAR,
            country VARCHAR,
            published_at TIMESTAMP,
            thumbnail_url VARCHAR,
            banner_url VARCHAR,
            keywords VARCHAR,
            topic_categories VARCHAR[],  -- Array of category URLs
            uploads_playlist_id VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Channel stats time series (append-only)
        CREATE TABLE IF NOT EXISTS channel_stats (
            channel_id VARCHAR,
            fetched_at TIMESTAMP,
            subscriber_count BIGINT,
            view_count BIGINT,
            video_count INTEGER,
            PRIMARY KEY (channel_id, fetched_at)
        );
        
        -- Videos dimension table (upserted on each fetch)
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR PRIMARY KEY,
            channel_id VARCHAR,
            title VARCHAR,
            description VARCHAR,
            published_at TIMESTAMP,
            duration_seconds INTEGER,
            duration_iso VARCHAR,
            category_id VARCHAR,
            default_language VARCHAR,
            default_audio_language VARCHAR,
            tags VARCHAR[],
            thumbnail_url VARCHAR,
            caption_available BOOLEAN,
            definition VARCHAR,  -- 'hd' or 'sd'
            dimension VARCHAR,   -- '2d' or '3d'
            projection VARCHAR,  -- 'rectangular' or '360'
            privacy_status VARCHAR,
            license VARCHAR,
            embeddable BOOLEAN,
            made_for_kids BOOLEAN,
            topic_categories VARCHAR[],
            has_chapters BOOLEAN DEFAULT FALSE,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Video stats time series (append-only)
        CREATE TABLE IF NOT EXISTS video_stats (
            video_id VARCHAR,
            fetched_at TIMESTAMP,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            PRIMARY KEY (video_id, fetched_at)
        );
        
        -- Chapters (write-once per video, deleted and rewritten if changed)
        CREATE TABLE IF NOT EXISTS chapters (
            video_id VARCHAR,
            chapter_index INTEGER,
            title VARCHAR,
            start_seconds INTEGER,
            end_seconds INTEGER,
            PRIMARY KEY (video_id, chapter_index)
        );
        
        -- Transcripts (write-once, only fetch if not exists)
        CREATE TABLE IF NOT EXISTS transcripts (
            video_id VARCHAR PRIMARY KEY,
            language VARCHAR,
            language_code VARCHAR,
            transcript_type VARCHAR,  -- 'manual', 'auto-generated', 'translated'
            full_text VARCHAR,
            entries_json VARCHAR,     -- JSON array of {start, duration, end, text}
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Comments (append-only with deduplication)
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR PRIMARY KEY,
            video_id VARCHAR,
            parent_comment_id VARCHAR,  -- NULL for top-level, comment_id for replies
            author_display_name VARCHAR,
            author_channel_id VARCHAR,
            text VARCHAR,
            like_count INTEGER,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Playlists dimension table
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id VARCHAR PRIMARY KEY,
            channel_id VARCHAR,
            title VARCHAR,
            description VARCHAR,
            published_at TIMESTAMP,
            thumbnail_url VARCHAR,
            item_count INTEGER,
            privacy_status VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Fetch log for tracking runs and incremental fetches
        CREATE TABLE IF NOT EXISTS fetch_log (
            fetch_id INTEGER PRIMARY KEY,
            channel_id VARCHAR,
            fetch_type VARCHAR,  -- 'full', 'incremental', 'backfill'
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            videos_fetched INTEGER,
            comments_fetched INTEGER,
            transcripts_fetched INTEGER,
            errors VARCHAR,
            status VARCHAR  -- 'running', 'completed', 'failed'
        );
        
        -- Create sequence for fetch_log if not exists
        CREATE SEQUENCE IF NOT EXISTS fetch_log_seq START 1;
        
        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id);
        CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at);
        CREATE INDEX IF NOT EXISTS idx_video_stats_fetched ON video_stats(fetched_at);
        CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id);
        CREATE INDEX IF NOT EXISTS idx_comments_published ON comments(published_at);
        CREATE INDEX IF NOT EXISTS idx_channel_stats_fetched ON channel_stats(fetched_at);
    """)
    
    conn.commit()


def get_last_fetch_time(conn: duckdb.DuckDBPyConnection, channel_id: str) -> Optional[datetime]:
    """Get the last successful fetch time for a channel."""
    result = conn.execute("""
        SELECT MAX(completed_at) 
        FROM fetch_log 
        WHERE channel_id = ? AND status = 'completed'
    """, [channel_id]).fetchone()
    return result[0] if result and result[0] else None


def get_existing_video_ids(conn: duckdb.DuckDBPyConnection, channel_id: str) -> set[str]:
    """Get all video IDs already in database for a channel."""
    result = conn.execute("""
        SELECT video_id FROM videos WHERE channel_id = ?
    """, [channel_id]).fetchall()
    return {row[0] for row in result}


def get_videos_without_transcripts(conn: duckdb.DuckDBPyConnection, channel_id: str) -> list[str]:
    """Get video IDs that don't have transcripts yet."""
    result = conn.execute("""
        SELECT v.video_id 
        FROM videos v
        LEFT JOIN transcripts t ON v.video_id = t.video_id
        WHERE v.channel_id = ? AND t.video_id IS NULL
    """, [channel_id]).fetchall()
    return [row[0] for row in result]


def get_latest_comment_time(conn: duckdb.DuckDBPyConnection, video_id: str) -> Optional[datetime]:
    """Get the most recent comment timestamp for a video."""
    result = conn.execute("""
        SELECT MAX(published_at) FROM comments WHERE video_id = ?
    """, [video_id]).fetchone()
    return result[0] if result and result[0] else None


def start_fetch_log(conn: duckdb.DuckDBPyConnection, channel_id: str, fetch_type: str) -> int:
    """Start a fetch log entry, return the fetch_id."""
    result = conn.execute("""
        INSERT INTO fetch_log (fetch_id, channel_id, fetch_type, started_at, status)
        VALUES (nextval('fetch_log_seq'), ?, ?, CURRENT_TIMESTAMP, 'running')
        RETURNING fetch_id
    """, [channel_id, fetch_type]).fetchone()
    conn.commit()
    return result[0]


def complete_fetch_log(conn: duckdb.DuckDBPyConnection, fetch_id: int, 
                       videos: int, comments: int, transcripts: int, 
                       status: str = 'completed', errors: str = None) -> None:
    """Complete a fetch log entry."""
    conn.execute("""
        UPDATE fetch_log 
        SET completed_at = CURRENT_TIMESTAMP,
            videos_fetched = ?,
            comments_fetched = ?,
            transcripts_fetched = ?,
            status = ?,
            errors = ?
        WHERE fetch_id = ?
    """, [videos, comments, transcripts, status, errors, fetch_id])
    conn.commit()


def upsert_channel(conn: duckdb.DuckDBPyConnection, channel: dict) -> None:
    """Insert or update channel metadata."""
    conn.execute("""
        INSERT OR REPLACE INTO channels (
            channel_id, title, description, custom_url, country, published_at,
            thumbnail_url, banner_url, keywords, topic_categories, 
            uploads_playlist_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        channel['channel_id'],
        channel.get('title'),
        channel.get('description'),
        channel.get('custom_url'),
        channel.get('country'),
        channel.get('published_at'),
        channel.get('thumbnail_url'),
        channel.get('banner_url'),
        channel.get('keywords'),
        channel.get('topic_categories', []),
        channel.get('uploads_playlist_id')
    ])
    conn.commit()


def insert_channel_stats(conn: duckdb.DuckDBPyConnection, channel_id: str, stats: dict) -> None:
    """Insert channel stats snapshot."""
    conn.execute("""
        INSERT INTO channel_stats (channel_id, fetched_at, subscriber_count, view_count, video_count)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [
        channel_id,
        stats.get('subscriber_count', 0),
        stats.get('view_count', 0),
        stats.get('video_count', 0)
    ])
    conn.commit()


def upsert_video(conn: duckdb.DuckDBPyConnection, video: dict) -> None:
    """Insert or update video metadata."""
    conn.execute("""
        INSERT INTO videos (
            video_id, channel_id, title, description, published_at,
            duration_seconds, duration_iso, category_id, default_language,
            default_audio_language, tags, thumbnail_url, caption_available,
            definition, dimension, projection, privacy_status, license,
            embeddable, made_for_kids, topic_categories, has_chapters, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (video_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            tags = EXCLUDED.tags,
            thumbnail_url = EXCLUDED.thumbnail_url,
            caption_available = EXCLUDED.caption_available,
            privacy_status = EXCLUDED.privacy_status,
            has_chapters = EXCLUDED.has_chapters,
            updated_at = CURRENT_TIMESTAMP
    """, [
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
        video.get('tags', []),
        video.get('thumbnail_url'),
        video.get('caption_available'),
        video.get('definition'),
        video.get('dimension'),
        video.get('projection'),
        video.get('privacy_status'),
        video.get('license'),
        video.get('embeddable'),
        video.get('made_for_kids'),
        video.get('topic_categories', []),
        video.get('has_chapters', False)
    ])
    conn.commit()


def insert_video_stats(conn: duckdb.DuckDBPyConnection, video_id: str, stats: dict) -> None:
    """Insert video stats snapshot."""
    conn.execute("""
        INSERT INTO video_stats (video_id, fetched_at, view_count, like_count, comment_count)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [
        video_id,
        stats.get('view_count', 0),
        stats.get('like_count', 0),
        stats.get('comment_count', 0)
    ])
    conn.commit()


def upsert_chapters(conn: duckdb.DuckDBPyConnection, video_id: str, chapters: list[dict]) -> None:
    """Replace chapters for a video."""
    conn.execute("DELETE FROM chapters WHERE video_id = ?", [video_id])
    for i, chapter in enumerate(chapters):
        conn.execute("""
            INSERT INTO chapters (video_id, chapter_index, title, start_seconds, end_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, [
            video_id,
            i,
            chapter.get('title'),
            chapter.get('start_seconds'),
            chapter.get('end_seconds')
        ])
    conn.commit()


def insert_transcript(conn: duckdb.DuckDBPyConnection, video_id: str, transcript: dict) -> None:
    """Insert transcript (only if not exists)."""
    import json
    conn.execute("""
        INSERT INTO transcripts (video_id, language, language_code, transcript_type, full_text, entries_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [
        video_id,
        transcript.get('language'),
        transcript.get('language_code'),
        transcript.get('transcript_type'),
        transcript.get('full_text'),
        json.dumps(transcript.get('entries', []))
    ])
    conn.commit()


def insert_comments(conn: duckdb.DuckDBPyConnection, comments: list[dict]) -> int:
    """Insert comments, skip duplicates. Returns count of new comments."""
    new_count = 0
    for comment in comments:
        result = conn.execute("""
            INSERT INTO comments (
                comment_id, video_id, parent_comment_id, author_display_name,
                author_channel_id, text, like_count, published_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        """, [
            comment['comment_id'],
            comment['video_id'],
            comment.get('parent_comment_id'),
            comment.get('author_display_name'),
            comment.get('author_channel_id'),
            comment.get('text'),
            comment.get('like_count', 0),
            comment.get('published_at'),
            comment.get('updated_at')
        ])
        if result.rowcount > 0:
            new_count += 1
    conn.commit()
    return new_count


def upsert_playlist(conn: duckdb.DuckDBPyConnection, playlist: dict) -> None:
    """Insert or update playlist."""
    conn.execute("""
        INSERT OR REPLACE INTO playlists (
            playlist_id, channel_id, title, description, published_at,
            thumbnail_url, item_count, privacy_status, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [
        playlist['playlist_id'],
        playlist.get('channel_id'),
        playlist.get('title'),
        playlist.get('description'),
        playlist.get('published_at'),
        playlist.get('thumbnail_url'),
        playlist.get('item_count'),
        playlist.get('privacy_status')
    ])
    conn.commit()


def export_to_parquet(conn: duckdb.DuckDBPyConnection, output_dir: str = "exports") -> dict[str, str]:
    """Export all tables to Parquet files for external analysis."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    tables = ['channels', 'channel_stats', 'videos', 'video_stats', 
              'chapters', 'transcripts', 'comments', 'playlists']
    
    exported = {}
    for table in tables:
        output_path = f"{output_dir}/{table}.parquet"
        conn.execute(f"COPY {table} TO '{output_path}' (FORMAT PARQUET)")
        exported[table] = output_path
    
    return exported