#!/usr/bin/env python3
"""
PostgreSQL/Supabase database optimization script.

This script implements storage optimization for PostgreSQL databases (including Supabase):
1. Time-series data compression (remove unchanged stats, aggregate old data)
2. Thumbnail URL optimization (pattern-based storage)
3. Space reclamation via VACUUM

Differences from SQLite version:
- Uses PostgreSQL-specific table size queries
- Handles VACUUM outside of transactions
- Uses CTE-based deletions instead of rowid
- Compatible with Supabase managed PostgreSQL

Usage:
    python scripts/optimize_database_postgres.py --dry-run          # Show what would be optimized
    python scripts/optimize_database_postgres.py --compress-stats   # Compress time-series data
    python scripts/optimize_database_postgres.py --optimize-thumbs  # Optimize thumbnail URLs
    python scripts/optimize_database_postgres.py --all              # Run all optimizations
    python scripts/optimize_database_postgres.py --report           # Generate detailed report
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.database import get_connection, is_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgresOptimizer:
    """Optimize PostgreSQL/Supabase database storage"""

    def __init__(self, dry_run: bool = False):
        if not is_postgres():
            raise ValueError("This script requires PostgreSQL backend. Set database_backend: postgres in config.")

        self.dry_run = dry_run
        self.conn = get_connection()
        self.stats = {
            'video_stats_removed': 0,
            'channel_stats_removed': 0,
            'space_reclaimed_mb': 0,
            'thumbnails_optimized': 0,
            'before_size_mb': 0,
            'after_size_mb': 0,
            'table_sizes_before': {},
            'table_sizes_after': {}
        }

    def get_database_size(self) -> float:
        """Get current database size in MB"""
        query = "SELECT pg_database_size(current_database()) / 1024.0 / 1024.0 as size_mb"
        result = self.conn.execute(query).fetchone()
        return result[0] if result else 0

    def get_table_sizes(self) -> Dict[str, float]:
        """Get size of all tables in MB"""
        query = """
            SELECT
                tablename,
                pg_total_relation_size('public'||'.'||tablename)::numeric / 1024.0 / 1024.0 as size_mb
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY size_mb DESC
        """

        results = self.conn.execute(query).fetchall()
        return {row[0]: float(row[1]) for row in results}

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

        result = self.conn.execute(duplicates_query).fetchone()
        duplicates = result[0] if result else 0
        logger.info(f"Found {duplicates:,} duplicate stats entries")

        if duplicates > 0 and not self.dry_run:
            # Delete duplicates using CTE
            delete_duplicates = """
                DELETE FROM video_stats
                WHERE (video_id, fetched_at) IN (
                    SELECT video_id, fetched_at
                    FROM (
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
                    ) ranked
                    WHERE rn > 1
                    AND view_count = prev_views
                    AND like_count = prev_likes
                    AND comment_count = prev_comments
                )
            """
            self.conn.execute(delete_duplicates)
            self.conn.commit()
            logger.info(f"Removed {duplicates:,} duplicate entries")

        # Step 2: Aggregate old data
        cutoff_30_days = datetime.now() - timedelta(days=30)
        cutoff_90_days = datetime.now() - timedelta(days=90)
        cutoff_365_days = datetime.now() - timedelta(days=365)

        # For 30-90 day old data: keep only one entry per day
        old_hourly_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at BETWEEN %s AND %s
            AND (video_id, fetched_at) NOT IN (
                SELECT video_id, MIN(fetched_at)
                FROM video_stats
                WHERE fetched_at BETWEEN %s AND %s
                GROUP BY video_id, DATE(fetched_at)
            )
        """
        result = self.conn.execute(
            old_hourly_query,
            (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days)
        ).fetchone()
        old_hourly = result[0] if result else 0
        logger.info(f"Found {old_hourly:,} hourly snapshots in 30-90 day range (will keep daily)")

        if old_hourly > 0 and not self.dry_run:
            delete_hourly = """
                DELETE FROM video_stats
                WHERE fetched_at BETWEEN %s AND %s
                AND (video_id, fetched_at) NOT IN (
                    SELECT video_id, MIN(fetched_at)
                    FROM video_stats
                    WHERE fetched_at BETWEEN %s AND %s
                    GROUP BY video_id, DATE(fetched_at)
                )
            """
            self.conn.execute(delete_hourly, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days))
            self.conn.commit()
            logger.info(f"Removed {old_hourly:,} old hourly snapshots")

        # For 90-365 day old data: keep only one entry per week
        old_daily_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at BETWEEN %s AND %s
            AND (video_id, fetched_at) NOT IN (
                SELECT video_id, MIN(fetched_at)
                FROM video_stats
                WHERE fetched_at BETWEEN %s AND %s
                GROUP BY video_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(WEEK FROM fetched_at)
            )
        """
        result = self.conn.execute(
            old_daily_query,
            (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days)
        ).fetchone()
        old_daily = result[0] if result else 0
        logger.info(f"Found {old_daily:,} daily snapshots in 90-365 day range (will keep weekly)")

        if old_daily > 0 and not self.dry_run:
            delete_daily = """
                DELETE FROM video_stats
                WHERE fetched_at BETWEEN %s AND %s
                AND (video_id, fetched_at) NOT IN (
                    SELECT video_id, MIN(fetched_at)
                    FROM video_stats
                    WHERE fetched_at BETWEEN %s AND %s
                    GROUP BY video_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(WEEK FROM fetched_at)
                )
            """
            self.conn.execute(delete_daily, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days))
            self.conn.commit()
            logger.info(f"Removed {old_daily:,} old daily snapshots")

        # For > 365 day old data: keep only one entry per month
        old_weekly_query = """
            SELECT COUNT(*) FROM video_stats
            WHERE fetched_at < %s
            AND (video_id, fetched_at) NOT IN (
                SELECT video_id, MIN(fetched_at)
                FROM video_stats
                WHERE fetched_at < %s
                GROUP BY video_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(MONTH FROM fetched_at)
            )
        """
        result = self.conn.execute(old_weekly_query, (cutoff_365_days, cutoff_365_days)).fetchone()
        old_weekly = result[0] if result else 0
        logger.info(f"Found {old_weekly:,} weekly snapshots older than 365 days (will keep monthly)")

        if old_weekly > 0 and not self.dry_run:
            delete_weekly = """
                DELETE FROM video_stats
                WHERE fetched_at < %s
                AND (video_id, fetched_at) NOT IN (
                    SELECT video_id, MIN(fetched_at)
                    FROM video_stats
                    WHERE fetched_at < %s
                    GROUP BY video_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(MONTH FROM fetched_at)
                )
            """
            self.conn.execute(delete_weekly, (cutoff_365_days, cutoff_365_days))
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

        # Find duplicates
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

        result = self.conn.execute(duplicates_query).fetchone()
        duplicates = result[0] if result else 0
        logger.info(f"Found {duplicates:,} duplicate channel stats entries")

        if duplicates > 0 and not self.dry_run:
            delete_duplicates = """
                DELETE FROM channel_stats
                WHERE (channel_id, fetched_at) IN (
                    SELECT channel_id, fetched_at
                    FROM (
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
                    ) ranked
                    WHERE rn > 1
                    AND subscriber_count = prev_subs
                    AND view_count = prev_views
                    AND video_count = prev_videos
                )
            """
            self.conn.execute(delete_duplicates)
            self.conn.commit()
            logger.info(f"Removed {duplicates:,} duplicate entries")

        # Aggregate old data (same retention policy as video_stats)
        cutoff_30_days = datetime.now() - timedelta(days=30)
        cutoff_90_days = datetime.now() - timedelta(days=90)
        cutoff_365_days = datetime.now() - timedelta(days=365)

        # Keep daily for 30-90 days
        result = self.conn.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at BETWEEN %s AND %s
            AND (channel_id, fetched_at) NOT IN (
                SELECT channel_id, MIN(fetched_at)
                FROM channel_stats
                WHERE fetched_at BETWEEN %s AND %s
                GROUP BY channel_id, DATE(fetched_at)
            )
        """, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days)).fetchone()
        old_hourly = result[0] if result else 0

        if old_hourly > 0 and not self.dry_run:
            self.conn.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at BETWEEN %s AND %s
                AND (channel_id, fetched_at) NOT IN (
                    SELECT channel_id, MIN(fetched_at)
                    FROM channel_stats
                    WHERE fetched_at BETWEEN %s AND %s
                    GROUP BY channel_id, DATE(fetched_at)
                )
            """, (cutoff_90_days, cutoff_30_days, cutoff_90_days, cutoff_30_days))
            self.conn.commit()

        # Keep weekly for 90-365 days
        result = self.conn.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at BETWEEN %s AND %s
            AND (channel_id, fetched_at) NOT IN (
                SELECT channel_id, MIN(fetched_at)
                FROM channel_stats
                WHERE fetched_at BETWEEN %s AND %s
                GROUP BY channel_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(WEEK FROM fetched_at)
            )
        """, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days)).fetchone()
        old_daily = result[0] if result else 0

        if old_daily > 0 and not self.dry_run:
            self.conn.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at BETWEEN %s AND %s
                AND (channel_id, fetched_at) NOT IN (
                    SELECT channel_id, MIN(fetched_at)
                    FROM channel_stats
                    WHERE fetched_at BETWEEN %s AND %s
                    GROUP BY channel_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(WEEK FROM fetched_at)
                )
            """, (cutoff_365_days, cutoff_90_days, cutoff_365_days, cutoff_90_days))
            self.conn.commit()

        # Keep monthly for > 365 days
        result = self.conn.execute("""
            SELECT COUNT(*) FROM channel_stats
            WHERE fetched_at < %s
            AND (channel_id, fetched_at) NOT IN (
                SELECT channel_id, MIN(fetched_at)
                FROM channel_stats
                WHERE fetched_at < %s
                GROUP BY channel_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(MONTH FROM fetched_at)
            )
        """, (cutoff_365_days, cutoff_365_days)).fetchone()
        old_weekly = result[0] if result else 0

        if old_weekly > 0 and not self.dry_run:
            self.conn.execute("""
                DELETE FROM channel_stats
                WHERE fetched_at < %s
                AND (channel_id, fetched_at) NOT IN (
                    SELECT channel_id, MIN(fetched_at)
                    FROM channel_stats
                    WHERE fetched_at < %s
                    GROUP BY channel_id, EXTRACT(YEAR FROM fetched_at), EXTRACT(MONTH FROM fetched_at)
                )
            """, (cutoff_365_days, cutoff_365_days))
            self.conn.commit()

        total_removed = duplicates + old_hourly + old_daily + old_weekly
        self.stats['channel_stats_removed'] = total_removed

        return total_removed

    def optimize_thumbnails(self) -> int:
        """
        Optimize thumbnail storage by extracting quality indicator from URLs.

        Returns:
            Number of thumbnails optimized
        """
        logger.info("Analyzing thumbnail URLs...")

        # Check if columns exist, add if needed
        try:
            self.conn.execute("ALTER TABLE videos ADD COLUMN thumbnail_quality TEXT")
            self.conn.commit()
            logger.info("Added thumbnail_quality column to videos")
        except Exception as e:
            if "already exists" in str(e).lower():
                pass  # Column exists
            else:
                logger.warning(f"Could not add thumbnail_quality column: {e}")

        try:
            self.conn.execute("ALTER TABLE channels ADD COLUMN thumbnail_quality TEXT")
            self.conn.execute("ALTER TABLE channels ADD COLUMN banner_quality TEXT")
            self.conn.commit()
            logger.info("Added quality columns to channels")
        except Exception as e:
            if "already exists" in str(e).lower():
                pass
            else:
                logger.warning(f"Could not add channel quality columns: {e}")

        # Count videos to optimize
        videos_query = """
            SELECT video_id, thumbnail_url
            FROM videos
            WHERE thumbnail_url IS NOT NULL
            AND (thumbnail_quality IS NULL OR thumbnail_quality = '')
        """
        videos_to_update = self.conn.execute(videos_query).fetchall()
        logger.info(f"Found {len(videos_to_update):,} videos with thumbnails to optimize")

        if not self.dry_run and len(videos_to_update) > 0:
            for video_id, thumbnail_url in videos_to_update:
                if '/vi/' in thumbnail_url:
                    quality = thumbnail_url.split('/')[-1].replace('.jpg', '').replace('.webp', '')
                    self.conn.execute(
                        "UPDATE videos SET thumbnail_quality = %s WHERE video_id = %s",
                        (quality, video_id)
                    )
            self.conn.commit()
            logger.info(f"Optimized {len(videos_to_update):,} video thumbnails")

        # Optimize channel thumbnails
        channels_query = """
            SELECT channel_id, thumbnail_url, banner_url
            FROM channels
            WHERE (thumbnail_url IS NOT NULL OR banner_url IS NOT NULL)
            AND (thumbnail_quality IS NULL OR banner_quality IS NULL)
        """
        channels_to_update = self.conn.execute(channels_query).fetchall()

        if not self.dry_run and len(channels_to_update) > 0:
            for channel_id, thumb_url, banner_url in channels_to_update:
                thumb_quality = None
                banner_quality = None

                if thumb_url and '/yt3/' in thumb_url:
                    parts = thumb_url.split('/')
                    if len(parts) > 0:
                        thumb_quality = parts[-1].replace('.jpg', '')

                if banner_url:
                    parts = banner_url.split('/')
                    if len(parts) > 0:
                        banner_quality = parts[-1]

                self.conn.execute("""
                    UPDATE channels
                    SET thumbnail_quality = %s, banner_quality = %s
                    WHERE channel_id = %s
                """, (thumb_quality, banner_quality, channel_id))

            self.conn.commit()
            logger.info(f"Optimized {len(channels_to_update):,} channel thumbnails")

        self.stats['thumbnails_optimized'] = len(videos_to_update) + len(channels_to_update)
        return self.stats['thumbnails_optimized']

    def vacuum_database(self):
        """
        Reclaim space after deletions using PostgreSQL VACUUM.

        Note: VACUUM FULL requires exclusive lock and may take time.
        We use regular VACUUM which is less disruptive.
        """
        if self.dry_run:
            logger.info("Dry run: Would run VACUUM to reclaim space")
            return

        logger.info("Running VACUUM to reclaim space...")

        # VACUUM cannot run inside a transaction block in PostgreSQL
        # We need to get the raw connection and set autocommit
        try:
            # Get the underlying psycopg connection
            raw_conn = self.conn._conn
            old_autocommit = raw_conn.autocommit
            raw_conn.autocommit = True

            cursor = raw_conn.cursor()
            cursor.execute("VACUUM ANALYZE")
            cursor.close()

            raw_conn.autocommit = old_autocommit
            logger.info("VACUUM complete")

        except Exception as e:
            logger.warning(f"VACUUM failed: {e}")
            logger.info("Skipping VACUUM (may not have permissions on managed database)")

    def generate_report(self) -> Dict:
        """Generate detailed optimization report"""
        logger.info("Generating optimization report...")

        # Get table statistics
        table_stats = {}
        for table in ['channels', 'videos', 'video_stats', 'channel_stats',
                      'comments', 'transcripts', 'chapters', 'playlists']:
            try:
                count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                table_stats[table] = count_result[0] if count_result else 0
            except:
                table_stats[table] = 0

        # Get date ranges for time-series tables
        date_ranges = {}
        for table in ['video_stats', 'channel_stats']:
            try:
                query = f"""
                    SELECT
                        MIN(fetched_at) as earliest,
                        MAX(fetched_at) as latest,
                        COUNT(DISTINCT DATE(fetched_at)) as unique_days
                    FROM {table}
                """
                result = self.conn.execute(query).fetchone()
                if result:
                    date_ranges[table] = {
                        'earliest': str(result[0]),
                        'latest': str(result[1]),
                        'unique_days': result[2]
                    }
            except:
                pass

        report = {
            'timestamp': datetime.now().isoformat(),
            'database_type': 'PostgreSQL',
            'dry_run': self.dry_run,
            'optimization_stats': self.stats,
            'table_row_counts': table_stats,
            'date_ranges': date_ranges,
            'table_sizes_mb': self.stats.get('table_sizes_after', {}),
            'recommendations': []
        }

        # Add recommendations
        if self.stats['video_stats_removed'] > 1000:
            report['recommendations'].append(
                "Schedule daily optimization to prevent time-series bloat"
            )

        if table_stats.get('comments', 0) > 50000:
            report['recommendations'].append(
                "Consider implementing comment archival for comments > 90 days old"
            )

        return report

    def run_all_optimizations(self, generate_report: bool = False):
        """Run all optimization strategies"""
        logger.info("=" * 70)
        logger.info("STARTING DATABASE OPTIMIZATION")
        logger.info("=" * 70)

        # Capture before state
        self.stats['before_size_mb'] = self.get_database_size()
        self.stats['table_sizes_before'] = self.get_table_sizes()

        logger.info(f"Database size before: {self.stats['before_size_mb']:.2f} MB")
        logger.info("\nTable sizes before optimization:")
        for table, size in sorted(self.stats['table_sizes_before'].items(),
                                 key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"  {table}: {size:.2f} MB")

        # Run optimizations
        logger.info("\n" + "=" * 70)
        logger.info("Phase 1: Time-series compression")
        logger.info("=" * 70)
        self.compress_video_stats()
        self.compress_channel_stats()

        logger.info("\n" + "=" * 70)
        logger.info("Phase 2: Thumbnail optimization")
        logger.info("=" * 70)
        self.optimize_thumbnails()

        logger.info("\n" + "=" * 70)
        logger.info("Phase 3: Space reclamation")
        logger.info("=" * 70)
        self.vacuum_database()

        # Capture after state
        self.stats['after_size_mb'] = self.get_database_size()
        self.stats['table_sizes_after'] = self.get_table_sizes()
        self.stats['space_reclaimed_mb'] = self.stats['before_size_mb'] - self.stats['after_size_mb']

        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("OPTIMIZATION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Video stats entries removed:   {self.stats['video_stats_removed']:,}")
        logger.info(f"Channel stats entries removed: {self.stats['channel_stats_removed']:,}")
        logger.info(f"Thumbnails optimized:          {self.stats['thumbnails_optimized']:,}")
        logger.info(f"\nDatabase size before:          {self.stats['before_size_mb']:.2f} MB")
        logger.info(f"Database size after:           {self.stats['after_size_mb']:.2f} MB")
        logger.info(f"Space reclaimed:               {self.stats['space_reclaimed_mb']:.2f} MB")

        if self.stats['before_size_mb'] > 0:
            reduction_pct = (self.stats['space_reclaimed_mb'] / self.stats['before_size_mb']) * 100
            logger.info(f"Total reduction:               {reduction_pct:.1f}%")

        logger.info("\nTop 10 tables by size after optimization:")
        for table, size in sorted(self.stats['table_sizes_after'].items(),
                                 key=lambda x: x[1], reverse=True)[:10]:
            before_size = self.stats['table_sizes_before'].get(table, 0)
            change = size - before_size
            change_pct = (change / before_size * 100) if before_size > 0 else 0
            logger.info(f"  {table}: {size:.2f} MB ({change:+.2f} MB, {change_pct:+.1f}%)")

        logger.info("=" * 70)

        if generate_report:
            report = self.generate_report()
            report_file = Path(__file__).parent.parent / 'logs' / f'optimization_report_{datetime.now():%Y%m%d_%H%M%S}.json'
            report_file.parent.mkdir(exist_ok=True)

            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)

            logger.info(f"\nDetailed report saved to: {report_file}")

            return report


def main():
    parser = argparse.ArgumentParser(description="Optimize PostgreSQL/Supabase database storage")
    parser.add_argument('--dry-run', action='store_true',
                       help="Show what would be done without making changes")
    parser.add_argument('--compress-stats', action='store_true',
                       help="Compress time-series stats tables")
    parser.add_argument('--optimize-thumbs', action='store_true',
                       help="Optimize thumbnail storage")
    parser.add_argument('--all', action='store_true',
                       help="Run all optimizations")
    parser.add_argument('--report', action='store_true',
                       help="Generate detailed JSON report")

    args = parser.parse_args()

    if args.dry_run:
        logger.info("=" * 70)
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 70)

    try:
        optimizer = PostgresOptimizer(dry_run=args.dry_run)

        if args.all:
            optimizer.run_all_optimizations(generate_report=args.report)
        elif args.compress_stats:
            optimizer.compress_video_stats()
            optimizer.compress_channel_stats()
            optimizer.vacuum_database()
        elif args.optimize_thumbs:
            optimizer.optimize_thumbnails()
            optimizer.vacuum_database()
        elif args.report:
            report = optimizer.generate_report()
            print(json.dumps(report, indent=2))
        else:
            parser.print_help()

    except Exception as e:
        logger.error(f"Optimization failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
