#!/usr/bin/env python3
"""
Database optimization script for reducing storage while maintaining performance.

This script implements Phase 1 optimizations:
1. Time-series data compression (remove unchanged stats, aggregate old data)
2. Thumbnail URL optimization (pattern-based storage)
3. Automated cleanup scheduling

Usage:
    python scripts/optimize_database.py --dry-run          # Show what would be deleted
    python scripts/optimize_database.py --compress-stats   # Compress time-series data
    python scripts/optimize_database.py --optimize-thumbs  # Optimize thumbnail URLs
    python scripts/optimize_database.py --all              # Run all optimizations
"""

import argparse
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

from database import get_connection, get_cursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseOptimizer:
    """Optimize database storage while maintaining performance"""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.conn = get_connection()
        self.cursor = get_cursor()
        self.stats = {
            'video_stats_removed': 0,
            'channel_stats_removed': 0,
            'space_reclaimed_mb': 0,
            'thumbnails_optimized': 0
        }

    def get_database_size(self) -> float:
        """Get current database size in MB"""
        result = self.cursor.execute(
            "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
        ).fetchone()
        return result[0] / (1024 * 1024) if result else 0

    def compress_video_stats(self) -> int:
        """
        Compress video_stats table by:
        1. Removing entries where stats didn't change from previous snapshot
        2. Aggregating old data (daily for 30-90 days, weekly for 90-365 days)

        Returns:
            Number of rows removed
        """
        logger.info("Analyzing video_stats table...")

        # Step 1: Find duplicate stats (consecutive snapshots with identical values)
        duplicates_query = """
            WITH ranked_stats AS (
                SELECT
                    video_id,
                    fetched_at,
                    view_count,
                    like_count,
                    comment_count,
                    LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_views,
                    LAG(like_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_likes,
                    LAG(comment_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_comments,
                    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY fetched_at) as rn
                FROM video_stats
            )
            SELECT COUNT(*) as duplicates
            FROM ranked_stats
            WHERE rn > 1
            AND view_count = prev_views
            AND like_count = prev_likes
            AND comment_count = prev_comments
        """

        result = self.cursor.execute(duplicates_query).fetchone()
        duplicates = result[0] if result else 0
        logger.info(f"Found {duplicates:,} duplicate stats entries")

        if duplicates > 0 and not self.dry_run:
            # Delete duplicates
            delete_duplicates = """
                DELETE FROM video_stats
                WHERE rowid IN (
                    WITH ranked_stats AS (
                        SELECT
                            rowid,
                            video_id,
                            fetched_at,
                            view_count,
                            like_count,
                            comment_count,
                            LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_views,
                            LAG(like_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_likes,
                            LAG(comment_count) OVER (PARTITION BY video_id ORDER BY fetched_at) as prev_comments,
                            ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY fetched_at) as rn
                        FROM video_stats
                    )
                    SELECT rowid
                    FROM ranked_stats
                    WHERE rn > 1
                    AND view_count = prev_views
                    AND like_count = prev_likes
                    AND comment_count = prev_comments
                )
            """
            self.cursor.execute(delete_duplicates)
            self.conn.commit()
            logger.info(f"Removed {duplicates:,} duplicate entries")

        # Step 2: Aggregate old data (keep only daily snapshots for 30-90 day old data)
        cutoff_30_days = datetime.now() - timedelta(days=30)
        cutoff_90_days = datetime.now() - timedelta(days=90)
        cutoff_365_days = datetime.now() - timedelta(days=365)

        # For 30-90 day old data: keep only one entry per day
        old_hourly_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at BETWEEN ? AND ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM video_stats
                WHERE fetched_at BETWEEN ? AND ?
                GROUP BY video_id, DATE(fetched_at)
            )
        """
        result = self.cursor.execute(
            old_hourly_query,
            (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days)
        ).fetchone()
        old_hourly = result[0] if result else 0
        logger.info(f"Found {old_hourly:,} hourly snapshots in 30-90 day range (will keep daily)")

        if old_hourly > 0 and not self.dry_run:
            delete_hourly = """
                DELETE FROM video_stats
                WHERE fetched_at BETWEEN ? AND ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM video_stats
                    WHERE fetched_at BETWEEN ? AND ?
                    GROUP BY video_id, DATE(fetched_at)
                )
            """
            self.cursor.execute(delete_hourly, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days))
            self.conn.commit()
            logger.info(f"Removed {old_hourly:,} old hourly snapshots")

        # For 90-365 day old data: keep only one entry per week
        old_daily_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at BETWEEN ? AND ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM video_stats
                WHERE fetched_at BETWEEN ? AND ?
                GROUP BY video_id, strftime('%Y-%W', fetched_at)
            )
        """
        result = self.cursor.execute(
            old_daily_query,
            (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days)
        ).fetchone()
        old_daily = result[0] if result else 0
        logger.info(f"Found {old_daily:,} daily snapshots in 90-365 day range (will keep weekly)")

        if old_daily > 0 and not self.dry_run:
            delete_daily = """
                DELETE FROM video_stats
                WHERE fetched_at BETWEEN ? AND ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM video_stats
                    WHERE fetched_at BETWEEN ? AND ?
                    GROUP BY video_id, strftime('%Y-%W', fetched_at)
                )
            """
            self.cursor.execute(delete_daily, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days))
            self.conn.commit()
            logger.info(f"Removed {old_daily:,} old daily snapshots")

        # For > 365 day old data: keep only one entry per month
        old_weekly_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at < ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM video_stats
                WHERE fetched_at < ?
                GROUP BY video_id, strftime('%Y-%m', fetched_at)
            )
        """
        result = self.cursor.execute(old_weekly_query, (cutoff_365_days, cutoff_365_days)).fetchone()
        old_weekly = result[0] if result else 0
        logger.info(f"Found {old_weekly:,} weekly snapshots older than 365 days (will keep monthly)")

        if old_weekly > 0 and not self.dry_run:
            delete_weekly = """
                DELETE FROM video_stats
                WHERE fetched_at < ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM video_stats
                    WHERE fetched_at < ?
                    GROUP BY video_id, strftime('%Y-%m', fetched_at)
                )
            """
            self.cursor.execute(delete_weekly, (cutoff_365_days, cutoff_365_days))
            self.conn.commit()
            logger.info(f"Removed {old_weekly:,} old weekly snapshots")

        total_removed = duplicates + old_hourly + old_daily + old_weekly
        self.stats['video_stats_removed'] = total_removed

        return total_removed

    def compress_channel_stats(self) -> int:
        """
        Compress channel_stats table using same strategy as video_stats

        Returns:
            Number of rows removed
        """
        logger.info("Analyzing channel_stats table...")

        # Same logic as video_stats but for channels
        duplicates_query = """
            WITH ranked_stats AS (
                SELECT
                    channel_id,
                    fetched_at,
                    subscriber_count,
                    view_count,
                    video_count,
                    LAG(subscriber_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_subs,
                    LAG(view_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_views,
                    LAG(video_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_videos,
                    ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY fetched_at) as rn
                FROM channel_stats
            )
            SELECT COUNT(*) as duplicates
            FROM ranked_stats
            WHERE rn > 1
            AND subscriber_count = prev_subs
            AND view_count = prev_views
            AND video_count = prev_videos
        """

        result = self.cursor.execute(duplicates_query).fetchone()
        duplicates = result[0] if result else 0
        logger.info(f"Found {duplicates:,} duplicate channel stats entries")

        if duplicates > 0 and not self.dry_run:
            delete_duplicates = """
                DELETE FROM channel_stats
                WHERE rowid IN (
                    WITH ranked_stats AS (
                        SELECT
                            rowid,
                            channel_id,
                            fetched_at,
                            subscriber_count,
                            view_count,
                            video_count,
                            LAG(subscriber_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_subs,
                            LAG(view_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_views,
                            LAG(video_count) OVER (PARTITION BY channel_id ORDER BY fetched_at) as prev_videos,
                            ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY fetched_at) as rn
                        FROM channel_stats
                    )
                    SELECT rowid
                    FROM ranked_stats
                    WHERE rn > 1
                    AND subscriber_count = prev_subs
                    AND view_count = prev_views
                    AND video_count = prev_videos
                )
            """
            self.cursor.execute(delete_duplicates)
            self.conn.commit()
            logger.info(f"Removed {duplicates:,} duplicate entries")

        # Aggregate old data (same retention policy as video_stats)
        cutoff_30_days = datetime.now() - timedelta(days=30)
        cutoff_90_days = datetime.now() - timedelta(days=90)
        cutoff_365_days = datetime.now() - timedelta(days=365)

        # Keep daily for 30-90 days
        result = self.cursor.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at BETWEEN ? AND ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM channel_stats
                WHERE fetched_at BETWEEN ? AND ?
                GROUP BY channel_id, DATE(fetched_at)
            )
        """, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days)).fetchone()
        old_hourly = result[0] if result else 0

        if old_hourly > 0 and not self.dry_run:
            self.cursor.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at BETWEEN ? AND ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM channel_stats
                    WHERE fetched_at BETWEEN ? AND ?
                    GROUP BY channel_id, DATE(fetched_at)
                )
            """, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days))
            self.conn.commit()

        # Keep weekly for 90-365 days
        result = self.cursor.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at BETWEEN ? AND ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM channel_stats
                WHERE fetched_at BETWEEN ? AND ?
                GROUP BY channel_id, strftime('%Y-%W', fetched_at)
            )
        """, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days)).fetchone()
        old_daily = result[0] if result else 0

        if old_daily > 0 and not self.dry_run:
            self.cursor.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at BETWEEN ? AND ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM channel_stats
                    WHERE fetched_at BETWEEN ? AND ?
                    GROUP BY channel_id, strftime('%Y-%W', fetched_at)
                )
            """, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days))
            self.conn.commit()

        # Keep monthly for > 365 days
        result = self.cursor.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at < ?
            AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM channel_stats
                WHERE fetched_at < ?
                GROUP BY channel_id, strftime('%Y-%m', fetched_at)
            )
        """, (cutoff_365_days, cutoff_365_days)).fetchone()
        old_weekly = result[0] if result else 0

        if old_weekly > 0 and not self.dry_run:
            self.cursor.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at < ?
                AND rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM channel_stats
                    WHERE fetched_at < ?
                    GROUP BY channel_id, strftime('%Y-%m', fetched_at)
                )
            """, (cutoff_365_days, cutoff_365_days))
            self.conn.commit()

        total_removed = duplicates + old_hourly + old_daily + old_weekly
        self.stats['channel_stats_removed'] = total_removed

        return total_removed

    def optimize_thumbnails(self) -> int:
        """
        Optimize thumbnail storage by extracting quality indicator from URLs.
        YouTube thumbnails follow pattern: https://i.ytimg.com/vi/{video_id}/{quality}.jpg

        Returns:
            Number of thumbnails optimized
        """
        logger.info("Analyzing thumbnail URLs...")

        # Add new column for quality indicator if it doesn't exist
        try:
            self.cursor.execute("ALTER TABLE videos ADD COLUMN thumbnail_quality TEXT")
            self.conn.commit()
            logger.info("Added thumbnail_quality column")
        except sqlite3.OperationalError:
            # Column already exists
            pass

        # Extract quality from existing URLs
        query = """
            SELECT video_id, thumbnail_url
            FROM videos
            WHERE thumbnail_url IS NOT NULL
            AND (thumbnail_quality IS NULL OR thumbnail_quality = '')
        """

        videos_to_update = self.cursor.execute(query).fetchall()
        logger.info(f"Found {len(videos_to_update):,} videos with thumbnails to optimize")

        if not self.dry_run and len(videos_to_update) > 0:
            for video_id, thumbnail_url in videos_to_update:
                # Extract quality from URL
                # Pattern: https://i.ytimg.com/vi/VIDEO_ID/QUALITY.jpg
                if '/vi/' in thumbnail_url:
                    quality = thumbnail_url.split('/')[-1].replace('.jpg', '').replace('.webp', '')
                    self.cursor.execute(
                        "UPDATE videos SET thumbnail_quality = ? WHERE video_id = ?",
                        (quality, video_id)
                    )

            self.conn.commit()
            logger.info(f"Optimized {len(videos_to_update):,} thumbnail URLs")

        # Do the same for channels
        try:
            self.cursor.execute("ALTER TABLE channels ADD COLUMN thumbnail_quality TEXT")
            self.cursor.execute("ALTER TABLE channels ADD COLUMN banner_quality TEXT")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        channels_to_update = self.cursor.execute("""
            SELECT channel_id, thumbnail_url, banner_url
            FROM channels
            WHERE (thumbnail_url IS NOT NULL OR banner_url IS NOT NULL)
            AND (thumbnail_quality IS NULL OR banner_quality IS NULL)
        """).fetchall()

        if not self.dry_run and len(channels_to_update) > 0:
            for channel_id, thumb_url, banner_url in channels_to_update:
                thumb_quality = None
                banner_quality = None

                if thumb_url and '/yt3/' in thumb_url:
                    # Channel thumbnails: https://yt3.ggpht.com/.../s800-c-k-c0x00ffffff-no-rj.jpg
                    parts = thumb_url.split('/')
                    if len(parts) > 0:
                        thumb_quality = parts[-1].replace('.jpg', '')

                if banner_url:
                    parts = banner_url.split('/')
                    if len(parts) > 0:
                        banner_quality = parts[-1]

                self.cursor.execute("""
                    UPDATE channels
                    SET thumbnail_quality = ?, banner_quality = ?
                    WHERE channel_id = ?
                """, (thumb_quality, banner_quality, channel_id))

            self.conn.commit()
            logger.info(f"Optimized {len(channels_to_update):,} channel thumbnails/banners")

        self.stats['thumbnails_optimized'] = len(videos_to_update) + len(channels_to_update)
        return self.stats['thumbnails_optimized']

    def vacuum_database(self):
        """Reclaim space after deletions"""
        if self.dry_run:
            logger.info("Dry run: Would run VACUUM to reclaim space")
            return

        logger.info("Running VACUUM to reclaim space...")
        size_before = self.get_database_size()

        self.cursor.execute("VACUUM")
        self.conn.commit()

        size_after = self.get_database_size()
        space_reclaimed = size_before - size_after
        self.stats['space_reclaimed_mb'] = space_reclaimed

        logger.info(f"VACUUM complete. Reclaimed {space_reclaimed:.2f} MB")
        logger.info(f"Database size: {size_before:.2f} MB -> {size_after:.2f} MB")

    def run_all_optimizations(self):
        """Run all optimization strategies"""
        logger.info("Starting database optimization...")
        size_before = self.get_database_size()
        logger.info(f"Database size before: {size_before:.2f} MB")

        # Phase 1: Time-series compression
        self.compress_video_stats()
        self.compress_channel_stats()

        # Phase 2: Thumbnail optimization
        self.optimize_thumbnails()

        # Reclaim space
        self.vacuum_database()

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("OPTIMIZATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Video stats entries removed:   {self.stats['video_stats_removed']:,}")
        logger.info(f"Channel stats entries removed: {self.stats['channel_stats_removed']:,}")
        logger.info(f"Thumbnails optimized:          {self.stats['thumbnails_optimized']:,}")
        logger.info(f"Space reclaimed:               {self.stats['space_reclaimed_mb']:.2f} MB")
        logger.info(f"Total reduction:               {(self.stats['space_reclaimed_mb'] / size_before * 100):.1f}%")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Optimize database storage")
    parser.add_argument('--dry-run', action='store_true', help="Show what would be done without making changes")
    parser.add_argument('--compress-stats', action='store_true', help="Compress time-series stats tables")
    parser.add_argument('--optimize-thumbs', action='store_true', help="Optimize thumbnail storage")
    parser.add_argument('--all', action='store_true', help="Run all optimizations")

    args = parser.parse_args()

    optimizer = DatabaseOptimizer(dry_run=args.dry_run)

    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    if args.all:
        optimizer.run_all_optimizations()
    elif args.compress_stats:
        optimizer.compress_video_stats()
        optimizer.compress_channel_stats()
        optimizer.vacuum_database()
    elif args.optimize_thumbs:
        optimizer.optimize_thumbnails()
        optimizer.vacuum_database()
    else:
        # Show help if no options specified
        parser.print_help()


if __name__ == '__main__':
    main()
