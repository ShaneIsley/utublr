#!/usr/bin/env python3
"""
Database Sync Tool for migrating between database backends.

Supports syncing data between:
- Turso (libSQL) - Cloud SQLite
- PostgreSQL - Traditional database
- Local SQLite file

Usage:
    python sync_database.py --source turso --dest postgres
    python sync_database.py --source postgres --dest turso --tables channels,videos

Environment Variables (Source):
    SOURCE_DATABASE_BACKEND: "turso" or "postgres" (overrides --source)
    SOURCE_TURSO_DATABASE_URL: libsql://your-db.turso.io
    SOURCE_TURSO_AUTH_TOKEN: Your Turso auth token
    SOURCE_POSTGRES_URL: postgresql://user:pass@host:port/dbname

Environment Variables (Destination):
    DEST_DATABASE_BACKEND: "turso" or "postgres" (overrides --dest)
    DEST_TURSO_DATABASE_URL: libsql://your-db.turso.io
    DEST_TURSO_AUTH_TOKEN: Your Turso auth token
    DEST_POSTGRES_URL: postgresql://user:pass@host:port/dbname
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional

# Add the scripts directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logger import get_logger

log = get_logger(__name__)

# All tables in the schema with their primary keys
TABLES = {
    "channels": {"pk": ["channel_id"], "order_by": "channel_id"},
    "channel_stats": {"pk": ["channel_id", "fetched_at"], "order_by": "channel_id, fetched_at"},
    "videos": {"pk": ["video_id"], "order_by": "video_id"},
    "video_stats": {"pk": ["video_id", "fetched_at"], "order_by": "video_id, fetched_at"},
    "chapters": {"pk": ["video_id", "chapter_index"], "order_by": "video_id, chapter_index"},
    "transcripts": {"pk": ["video_id"], "order_by": "video_id"},
    "comments": {"pk": ["comment_id"], "order_by": "comment_id"},
    "playlists": {"pk": ["playlist_id"], "order_by": "playlist_id"},
    "fetch_log": {"pk": ["fetch_id"], "order_by": "fetch_id", "autoincrement": True},
    "fetch_progress": {"pk": ["channel_id", "fetch_id", "operation"], "order_by": "channel_id, fetch_id"},
    "quota_usage": {"pk": ["date"], "order_by": "date"},
}

# Batch size for data transfer
BATCH_SIZE = 1000


def get_source_connection():
    """Get connection to the source database."""
    backend = os.environ.get("SOURCE_DATABASE_BACKEND", "turso")

    if backend == "postgres":
        conn_string = os.environ.get("SOURCE_POSTGRES_URL")
        if not conn_string:
            raise ValueError("SOURCE_POSTGRES_URL required for postgres source")

        import psycopg
        log.info(f"Connecting to source PostgreSQL: {conn_string[:40]}...")
        return psycopg.connect(conn_string), "postgres"
    else:
        import libsql
        url = os.environ.get("SOURCE_TURSO_DATABASE_URL")
        auth_token = os.environ.get("SOURCE_TURSO_AUTH_TOKEN", "")

        if not url:
            raise ValueError("SOURCE_TURSO_DATABASE_URL required for turso source")

        log.info(f"Connecting to source Turso: {url[:40]}...")
        if url.startswith("libsql://") or url.startswith("https://"):
            conn = libsql.connect(database=url, auth_token=auth_token)
        else:
            conn = libsql.connect(database=url)
        return conn, "turso"


def get_dest_connection():
    """Get connection to the destination database."""
    backend = os.environ.get("DEST_DATABASE_BACKEND", "postgres")

    if backend == "postgres":
        conn_string = os.environ.get("DEST_POSTGRES_URL")
        if not conn_string:
            raise ValueError("DEST_POSTGRES_URL required for postgres destination")

        import psycopg
        log.info(f"Connecting to destination PostgreSQL: {conn_string[:40]}...")
        return psycopg.connect(conn_string), "postgres"
    else:
        import libsql
        url = os.environ.get("DEST_TURSO_DATABASE_URL")
        auth_token = os.environ.get("DEST_TURSO_AUTH_TOKEN", "")

        if not url:
            raise ValueError("DEST_TURSO_DATABASE_URL required for turso destination")

        log.info(f"Connecting to destination Turso: {url[:40]}...")
        if url.startswith("libsql://") or url.startswith("https://"):
            conn = libsql.connect(database=url, auth_token=auth_token)
        else:
            conn = libsql.connect(database=url)
        return conn, "turso"


def create_tables_sqlite(conn) -> None:
    """Create all tables using SQLite/Turso syntax."""
    log.info("Creating tables in SQLite/Turso...")

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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_published ON videos(channel_id, published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_video_published ON comments(video_id, published_at)")

    conn.commit()
    log.info("SQLite/Turso tables created successfully")


def create_tables_postgres(conn) -> None:
    """Create all tables using PostgreSQL syntax."""
    log.info("Creating tables in PostgreSQL...")

    cursor = conn.cursor()

    cursor.execute("""
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_stats (
            channel_id TEXT,
            fetched_at TEXT,
            subscriber_count BIGINT,
            view_count BIGINT,
            video_count INTEGER,
            PRIMARY KEY (channel_id, fetched_at)
        )
    """)

    cursor.execute("""
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_stats (
            video_id TEXT,
            fetched_at TEXT,
            view_count BIGINT,
            like_count BIGINT,
            comment_count INTEGER,
            PRIMARY KEY (video_id, fetched_at)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chapters (
            video_id TEXT,
            chapter_index INTEGER,
            title TEXT,
            start_seconds INTEGER,
            end_seconds INTEGER,
            PRIMARY KEY (video_id, chapter_index)
        )
    """)

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quota_usage (
            date TEXT PRIMARY KEY,
            used INTEGER,
            operations TEXT,
            last_updated TEXT
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_fetched ON video_stats(fetched_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_stats_video ON video_stats(video_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_published ON comments(published_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_stats_fetched ON channel_stats(fetched_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_channel_published ON videos(channel_id, published_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_video_published ON comments(video_id, published_at)")

    conn.commit()
    log.info("PostgreSQL tables created successfully")


def get_table_columns(conn, table: str, backend: str) -> list:
    """Get column names for a table."""
    if backend == "postgres":
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        return [row[0] for row in cursor.fetchall()]
    else:
        # SQLite/Turso
        cursor = conn.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in cursor.fetchall()]


def count_rows(conn, table: str, backend: str) -> int:
    """Count rows in a table."""
    if backend == "postgres":
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]
    else:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]


def fetch_rows(conn, table: str, backend: str, offset: int, limit: int, order_by: str) -> list:
    """Fetch rows from a table with pagination."""
    if backend == "postgres":
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table} ORDER BY {order_by} LIMIT %s OFFSET %s", (limit, offset))
        return cursor.fetchall()
    else:
        cursor = conn.execute(f"SELECT * FROM {table} ORDER BY {order_by} LIMIT ? OFFSET ?", (limit, offset))
        return cursor.fetchall()


def insert_rows_postgres(conn, table: str, columns: list, rows: list, pk_columns: list) -> int:
    """Insert rows into PostgreSQL with ON CONFLICT DO UPDATE."""
    if not rows:
        return 0

    cursor = conn.cursor()

    # Build the INSERT ... ON CONFLICT statement
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    pk_list = ", ".join(pk_columns)

    # Build UPDATE clause for non-PK columns
    non_pk_columns = [c for c in columns if c not in pk_columns]
    if non_pk_columns:
        update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_columns])
        sql = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}
        """
    else:
        # All columns are PKs, just ignore conflicts
        sql = f"""
            INSERT INTO {table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({pk_list}) DO NOTHING
        """

    inserted = 0
    for row in rows:
        try:
            cursor.execute(sql, row)
            inserted += 1
        except Exception as e:
            log.warning(f"Error inserting row into {table}: {e}")

    conn.commit()
    return inserted


def insert_rows_sqlite(conn, table: str, columns: list, rows: list, pk_columns: list) -> int:
    """Insert rows into SQLite/Turso with INSERT OR REPLACE."""
    if not rows:
        return 0

    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(columns)

    sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"

    inserted = 0
    for row in rows:
        try:
            conn.execute(sql, row)
            inserted += 1
        except Exception as e:
            log.warning(f"Error inserting row into {table}: {e}")

    conn.commit()
    return inserted


def sync_table(
    source_conn,
    source_backend: str,
    dest_conn,
    dest_backend: str,
    table: str,
    table_info: dict,
    dry_run: bool = False
) -> dict:
    """Sync a single table from source to destination."""
    log.info(f"Syncing table: {table}")

    # Get column information from source
    source_columns = get_table_columns(source_conn, table, source_backend)

    if not source_columns:
        log.warning(f"Table {table} not found in source database")
        return {"table": table, "status": "skipped", "reason": "not found in source"}

    # Count source rows
    source_count = count_rows(source_conn, table, source_backend)
    log.info(f"  Source has {source_count} rows")

    if dry_run:
        return {
            "table": table,
            "status": "dry_run",
            "source_count": source_count,
            "synced": 0
        }

    # Get destination columns
    dest_columns = get_table_columns(dest_conn, table, dest_backend)

    # Find common columns (handle schema differences)
    common_columns = [c for c in source_columns if c in dest_columns]
    if len(common_columns) < len(source_columns):
        missing = set(source_columns) - set(common_columns)
        log.warning(f"  Columns missing in destination: {missing}")

    pk_columns = table_info["pk"]
    order_by = table_info["order_by"]

    # Sync in batches
    total_synced = 0
    offset = 0

    while offset < source_count:
        rows = fetch_rows(source_conn, table, source_backend, offset, BATCH_SIZE, order_by)
        if not rows:
            break

        # Get column indices for common columns
        col_indices = [source_columns.index(c) for c in common_columns]

        # Extract only common columns from rows
        filtered_rows = [tuple(row[i] for i in col_indices) for row in rows]

        # Insert into destination
        if dest_backend == "postgres":
            inserted = insert_rows_postgres(dest_conn, table, common_columns, filtered_rows, pk_columns)
        else:
            inserted = insert_rows_sqlite(dest_conn, table, common_columns, filtered_rows, pk_columns)

        total_synced += inserted
        offset += len(rows)

        if offset % (BATCH_SIZE * 10) == 0 or offset >= source_count:
            log.info(f"  Progress: {offset}/{source_count} rows processed, {total_synced} synced")

    log.info(f"  Completed: {total_synced} rows synced")

    return {
        "table": table,
        "status": "success",
        "source_count": source_count,
        "synced": total_synced
    }


def sync_databases(
    tables_to_sync: Optional[list] = None,
    create_tables: bool = True,
    dry_run: bool = False
) -> dict:
    """
    Sync data from source database to destination database.

    Args:
        tables_to_sync: List of table names to sync (None = all tables)
        create_tables: Whether to create tables in destination if they don't exist
        dry_run: If True, only report what would be synced

    Returns:
        Dictionary with sync results
    """
    start_time = time.time()
    results = {
        "started_at": datetime.utcnow().isoformat(),
        "tables": [],
        "status": "success"
    }

    try:
        # Connect to databases
        source_conn, source_backend = get_source_connection()
        dest_conn, dest_backend = get_dest_connection()

        results["source_backend"] = source_backend
        results["dest_backend"] = dest_backend

        log.info(f"Source: {source_backend}, Destination: {dest_backend}")

        # Create tables in destination if requested
        if create_tables and not dry_run:
            if dest_backend == "postgres":
                create_tables_postgres(dest_conn)
            else:
                create_tables_sqlite(dest_conn)

        # Determine which tables to sync
        if tables_to_sync:
            tables = {t: TABLES[t] for t in tables_to_sync if t in TABLES}
            if len(tables) < len(tables_to_sync):
                unknown = set(tables_to_sync) - set(TABLES.keys())
                log.warning(f"Unknown tables ignored: {unknown}")
        else:
            tables = TABLES

        # Sync each table
        for table_name, table_info in tables.items():
            try:
                result = sync_table(
                    source_conn, source_backend,
                    dest_conn, dest_backend,
                    table_name, table_info,
                    dry_run=dry_run
                )
                results["tables"].append(result)
            except Exception as e:
                log.error(f"Error syncing table {table_name}: {e}")
                results["tables"].append({
                    "table": table_name,
                    "status": "error",
                    "error": str(e)
                })
                results["status"] = "partial"

        # Close connections
        try:
            if source_backend == "postgres":
                source_conn.close()
            if dest_backend == "postgres":
                dest_conn.close()
        except:
            pass

    except Exception as e:
        log.error(f"Sync failed: {e}")
        results["status"] = "failed"
        results["error"] = str(e)

    results["completed_at"] = datetime.utcnow().isoformat()
    results["duration_seconds"] = round(time.time() - start_time, 2)

    return results


def print_results(results: dict) -> None:
    """Print sync results in a formatted way."""
    print("\n" + "=" * 60)
    print("DATABASE SYNC RESULTS")
    print("=" * 60)
    print(f"Source: {results.get('source_backend', 'unknown')}")
    print(f"Destination: {results.get('dest_backend', 'unknown')}")
    print(f"Started: {results.get('started_at', 'unknown')}")
    print(f"Duration: {results.get('duration_seconds', 0)} seconds")
    print(f"Status: {results.get('status', 'unknown')}")
    print()

    if results.get("tables"):
        print("Table Results:")
        print("-" * 60)
        total_source = 0
        total_synced = 0

        for table_result in results["tables"]:
            table = table_result["table"]
            status = table_result["status"]
            source_count = table_result.get("source_count", 0)
            synced = table_result.get("synced", 0)

            total_source += source_count
            total_synced += synced

            if status == "success":
                print(f"  {table}: {synced}/{source_count} rows synced")
            elif status == "dry_run":
                print(f"  {table}: {source_count} rows (dry run)")
            elif status == "skipped":
                print(f"  {table}: skipped - {table_result.get('reason', 'unknown')}")
            elif status == "error":
                print(f"  {table}: ERROR - {table_result.get('error', 'unknown')}")

        print("-" * 60)
        print(f"Total: {total_synced}/{total_source} rows synced")

    if results.get("error"):
        print(f"\nError: {results['error']}")

    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Sync data between database backends (Turso, PostgreSQL, SQLite)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync from Turso to PostgreSQL
  SOURCE_DATABASE_BACKEND=turso SOURCE_TURSO_DATABASE_URL=libsql://... \\
  DEST_DATABASE_BACKEND=postgres DEST_POSTGRES_URL=postgresql://... \\
  python sync_database.py

  # Sync specific tables only
  python sync_database.py --tables channels,videos,video_stats

  # Dry run to see what would be synced
  python sync_database.py --dry-run

  # Sync without creating tables (assumes they exist)
  python sync_database.py --no-create-tables
        """
    )

    parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of tables to sync (default: all)",
        default=None
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would be synced, don't actually sync"
    )

    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        help="Don't create tables in destination (assumes they exist)"
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )

    args = parser.parse_args()

    # Parse tables
    tables_to_sync = None
    if args.tables:
        tables_to_sync = [t.strip() for t in args.tables.split(",")]

    # Run sync
    results = sync_databases(
        tables_to_sync=tables_to_sync,
        create_tables=not args.no_create_tables,
        dry_run=args.dry_run
    )

    # Output results
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_results(results)

    # Exit with appropriate code
    if results["status"] == "failed":
        sys.exit(1)
    elif results["status"] == "partial":
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
