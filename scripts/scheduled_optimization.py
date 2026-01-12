#!/usr/bin/env python3
"""
Scheduled database optimization tasks.

This script is designed to run periodically (via cron or systemd timer) to:
1. Compress old time-series data
2. Archive old comments
3. Maintain optimal database size

Recommended schedule:
- Daily: Compress stats older than 30 days
- Weekly: Archive comments older than 90 days
- Monthly: Full VACUUM operation

Example crontab entries:
    # Daily at 2 AM: Compress old stats
    0 2 * * * /path/to/utublr/scripts/scheduled_optimization.py --daily

    # Weekly on Sunday at 3 AM: Archive comments
    0 3 * * 0 /path/to/utublr/scripts/scheduled_optimization.py --weekly

    # Monthly on 1st at 4 AM: Full maintenance
    0 4 1 * * /path/to/utublr/scripts/scheduled_optimization.py --monthly
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.database import get_connection, get_cursor
from scripts.optimize_database import DatabaseOptimizer

# Configure logging with file output
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'optimization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ScheduledMaintenance:
    """Scheduled database maintenance operations"""

    def __init__(self):
        self.conn = get_connection()
        self.cursor = get_cursor()
        self.optimizer = DatabaseOptimizer(dry_run=False)

    def daily_maintenance(self):
        """Run daily optimization tasks"""
        logger.info("=" * 60)
        logger.info("DAILY MAINTENANCE STARTED")
        logger.info("=" * 60)

        # Compress stats older than 30 days
        logger.info("Compressing video stats...")
        video_stats_removed = self.optimizer.compress_video_stats()

        logger.info("Compressing channel stats...")
        channel_stats_removed = self.optimizer.compress_channel_stats()

        # Quick VACUUM if significant data removed
        total_removed = video_stats_removed + channel_stats_removed
        if total_removed > 1000:
            logger.info(f"Removed {total_removed:,} rows, running VACUUM...")
            self.optimizer.vacuum_database()

        logger.info("Daily maintenance complete")

    def weekly_maintenance(self):
        """Run weekly optimization tasks"""
        logger.info("=" * 60)
        logger.info("WEEKLY MAINTENANCE STARTED")
        logger.info("=" * 60)

        # Archive old comments (if comment archival is implemented)
        self.archive_old_comments()

        # Full stats compression
        self.daily_maintenance()

        # VACUUM to reclaim space
        logger.info("Running weekly VACUUM...")
        self.optimizer.vacuum_database()

        logger.info("Weekly maintenance complete")

    def monthly_maintenance(self):
        """Run monthly optimization tasks"""
        logger.info("=" * 60)
        logger.info("MONTHLY MAINTENANCE STARTED")
        logger.info("=" * 60)

        # Run database health checks
        self.run_health_checks()

        # Full optimization
        self.optimizer.run_all_optimizations()

        # Analyze database statistics
        self.analyze_statistics()

        logger.info("Monthly maintenance complete")

    def archive_old_comments(self):
        """
        Archive comments older than 90 days to reduce table size.
        Keeps summary statistics and top comments.
        """
        logger.info("Checking for comments to archive...")

        cutoff_date = datetime.now() - timedelta(days=90)

        # Count comments to archive
        count_query = """
            SELECT COUNT(*) FROM comments
            WHERE published_at < ?
        """
        result = self.cursor.execute(count_query, (cutoff_date,)).fetchone()
        old_comments = result[0] if result else 0

        if old_comments == 0:
            logger.info("No comments to archive")
            return

        logger.info(f"Found {old_comments:,} comments older than 90 days")

        # Create archive table if it doesn't exist
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments_archive (
                video_id TEXT NOT NULL,
                archived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                original_count INTEGER NOT NULL,
                total_likes INTEGER NOT NULL,
                avg_likes_per_comment REAL,
                top_comments TEXT,  -- JSON array of top 10 most-liked comments
                PRIMARY KEY (video_id, archived_at)
            )
        """)

        # Archive old comments by video
        archive_query = """
            INSERT INTO comments_archive (
                video_id,
                archived_at,
                original_count,
                total_likes,
                avg_likes_per_comment,
                top_comments
            )
            SELECT
                video_id,
                CURRENT_TIMESTAMP,
                COUNT(*) as original_count,
                SUM(like_count) as total_likes,
                AVG(like_count) as avg_likes_per_comment,
                json_group_array(
                    json_object(
                        'author', author_display_name,
                        'text', SUBSTR(text, 1, 200),  -- First 200 chars
                        'likes', like_count,
                        'published', published_at
                    )
                ) FILTER (
                    WHERE rowid IN (
                        SELECT rowid FROM comments c2
                        WHERE c2.video_id = comments.video_id
                        AND c2.published_at < ?
                        ORDER BY like_count DESC
                        LIMIT 10
                    )
                ) as top_comments
            FROM comments
            WHERE published_at < ?
            GROUP BY video_id
        """

        self.cursor.execute(archive_query, (cutoff_date, cutoff_date))

        # Delete archived comments
        delete_query = "DELETE FROM comments WHERE published_at < ?"
        self.cursor.execute(delete_query, (cutoff_date,))

        self.conn.commit()
        logger.info(f"Archived and removed {old_comments:,} old comments")

    def run_health_checks(self):
        """Run database integrity and health checks"""
        logger.info("Running database health checks...")

        # Check for orphaned records
        orphaned_checks = [
            ("videos without channels", """
                SELECT COUNT(*) FROM videos v
                WHERE NOT EXISTS (SELECT 1 FROM channels c WHERE c.channel_id = v.channel_id)
            """),
            ("comments without videos", """
                SELECT COUNT(*) FROM comments c
                WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.video_id = c.video_id)
            """),
            ("video_stats without videos", """
                SELECT COUNT(*) FROM video_stats vs
                WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.video_id = vs.video_id)
            """),
        ]

        for check_name, query in orphaned_checks:
            result = self.cursor.execute(query).fetchone()
            count = result[0] if result else 0
            if count > 0:
                logger.warning(f"Found {count:,} {check_name}")
            else:
                logger.info(f"✓ No {check_name}")

        # Check index usage
        logger.info("Analyzing index usage...")
        self.cursor.execute("ANALYZE")
        self.conn.commit()

    def analyze_statistics(self):
        """Analyze and log database statistics"""
        logger.info("Database Statistics:")

        stats_queries = [
            ("Total channels", "SELECT COUNT(*) FROM channels"),
            ("Total videos", "SELECT COUNT(*) FROM videos"),
            ("Total comments", "SELECT COUNT(*) FROM comments"),
            ("Total transcripts", "SELECT COUNT(*) FROM transcripts"),
            ("Video stats entries", "SELECT COUNT(*) FROM video_stats"),
            ("Channel stats entries", "SELECT COUNT(*) FROM channel_stats"),
        ]

        for stat_name, query in stats_queries:
            result = self.cursor.execute(query).fetchone()
            count = result[0] if result else 0
            logger.info(f"  {stat_name}: {count:,}")

        # Storage statistics
        size_query = """
            SELECT
                name as table_name,
                ROUND(SUM(pgsize)/1024.0/1024.0, 2) as size_mb
            FROM dbstat
            WHERE name NOT LIKE 'sqlite_%'
            GROUP BY name
            ORDER BY size_mb DESC
            LIMIT 10
        """

        logger.info("\nTop 10 tables by size:")
        for row in self.cursor.execute(size_query).fetchall():
            logger.info(f"  {row[0]}: {row[1]:.2f} MB")


def main():
    parser = argparse.ArgumentParser(description="Run scheduled database maintenance")
    parser.add_argument('--daily', action='store_true', help="Run daily maintenance")
    parser.add_argument('--weekly', action='store_true', help="Run weekly maintenance")
    parser.add_argument('--monthly', action='store_true', help="Run monthly maintenance")

    args = parser.parse_args()

    if not any([args.daily, args.weekly, args.monthly]):
        parser.print_help()
        return

    maintenance = ScheduledMaintenance()

    try:
        if args.daily:
            maintenance.daily_maintenance()
        elif args.weekly:
            maintenance.weekly_maintenance()
        elif args.monthly:
            maintenance.monthly_maintenance()

        logger.info("All maintenance tasks completed successfully")

    except Exception as e:
        logger.error(f"Maintenance failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
