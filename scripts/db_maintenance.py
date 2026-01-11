#!/usr/bin/env python3
"""
Database Maintenance and Completeness Analysis Tool

Performs health checks, repairs, and completeness analysis on the database.

Features:
- Repair video_comment_summary table (reconcile counts with actual data)
- Detect orphaned records (stats without videos, comments without videos, etc.)
- Per-channel completeness analysis (videos, stats, comments, transcripts)
- Summary health report

Usage:
    # Full health check and report
    python db_maintenance.py --check

    # Repair summary table
    python db_maintenance.py --repair

    # Completeness analysis
    python db_maintenance.py --completeness

    # All operations
    python db_maintenance.py --all

    # Output as JSON (for CI/automation)
    python db_maintenance.py --all --json
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

from database import get_connection, init_database, is_postgres
from logger import get_logger

log = get_logger("db_maintenance")


@dataclass
class RepairResult:
    """Results from a repair operation."""
    operation: str
    records_checked: int = 0
    records_fixed: int = 0
    errors: list = field(default_factory=list)
    duration_ms: float = 0


@dataclass
class HealthCheck:
    """Results from a health check."""
    check_name: str
    status: str  # "ok", "warning", "error"
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class ChannelCompleteness:
    """Completeness metrics for a single channel."""
    channel_id: str
    channel_title: str
    # Video metrics
    total_videos: int = 0
    # Stats coverage
    videos_with_stats: int = 0
    stats_coverage_pct: float = 0.0
    # Comment coverage
    videos_with_comments: int = 0
    total_comments_stored: int = 0
    total_comments_youtube: int = 0
    comment_coverage_pct: float = 0.0
    # Transcript coverage
    videos_with_transcripts: int = 0
    transcript_coverage_pct: float = 0.0


@dataclass
class MaintenanceReport:
    """Full maintenance report."""
    timestamp: str
    duration_ms: float = 0
    health_checks: list = field(default_factory=list)
    repairs: list = field(default_factory=list)
    channel_completeness: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)


# ============================================================================
# HEALTH CHECKS
# ============================================================================

def check_orphaned_video_stats(conn) -> HealthCheck:
    """Check for video_stats records without corresponding videos."""
    result = conn.execute("""
        SELECT COUNT(*) FROM video_stats vs
        LEFT JOIN videos v ON vs.video_id = v.video_id
        WHERE v.video_id IS NULL
    """).fetchone()

    orphan_count = result[0] if result else 0

    if orphan_count == 0:
        return HealthCheck("orphaned_video_stats", "ok", "No orphaned video stats found")
    else:
        return HealthCheck(
            "orphaned_video_stats",
            "warning",
            f"Found {orphan_count} video_stats records without corresponding videos",
            {"orphan_count": orphan_count}
        )


def check_orphaned_comments(conn) -> HealthCheck:
    """Check for comments without corresponding videos."""
    result = conn.execute("""
        SELECT COUNT(*) FROM comments c
        LEFT JOIN videos v ON c.video_id = v.video_id
        WHERE v.video_id IS NULL
    """).fetchone()

    orphan_count = result[0] if result else 0

    if orphan_count == 0:
        return HealthCheck("orphaned_comments", "ok", "No orphaned comments found")
    else:
        return HealthCheck(
            "orphaned_comments",
            "warning",
            f"Found {orphan_count} comments without corresponding videos",
            {"orphan_count": orphan_count}
        )


def check_orphaned_transcripts(conn) -> HealthCheck:
    """Check for transcripts without corresponding videos."""
    result = conn.execute("""
        SELECT COUNT(*) FROM transcripts t
        LEFT JOIN videos v ON t.video_id = v.video_id
        WHERE v.video_id IS NULL
    """).fetchone()

    orphan_count = result[0] if result else 0

    if orphan_count == 0:
        return HealthCheck("orphaned_transcripts", "ok", "No orphaned transcripts found")
    else:
        return HealthCheck(
            "orphaned_transcripts",
            "warning",
            f"Found {orphan_count} transcripts without corresponding videos",
            {"orphan_count": orphan_count}
        )


def check_summary_table_accuracy(conn) -> HealthCheck:
    """Check if video_comment_summary matches actual counts."""
    # Sample check - compare summary counts with actual for a subset
    result = conn.execute("""
        SELECT COUNT(*) as mismatches FROM (
            SELECT s.video_id,
                   s.stored_comment_count as summary_count,
                   COALESCE(c.actual_count, 0) as actual_count
            FROM video_comment_summary s
            LEFT JOIN (
                SELECT video_id, COUNT(*) as actual_count
                FROM comments
                GROUP BY video_id
            ) c ON s.video_id = c.video_id
            WHERE s.stored_comment_count != COALESCE(c.actual_count, 0)
            LIMIT 1000
        )
    """).fetchone()

    mismatch_count = result[0] if result else 0

    if mismatch_count == 0:
        return HealthCheck(
            "summary_table_accuracy",
            "ok",
            "Summary table counts match actual data"
        )
    else:
        return HealthCheck(
            "summary_table_accuracy",
            "warning",
            f"Found {mismatch_count}+ summary records with incorrect counts",
            {"mismatch_count": mismatch_count}
        )


def check_videos_without_channels(conn) -> HealthCheck:
    """Check for videos without corresponding channels."""
    result = conn.execute("""
        SELECT COUNT(*) FROM videos v
        LEFT JOIN channels c ON v.channel_id = c.channel_id
        WHERE c.channel_id IS NULL
    """).fetchone()

    orphan_count = result[0] if result else 0

    if orphan_count == 0:
        return HealthCheck("videos_without_channels", "ok", "All videos have channels")
    else:
        return HealthCheck(
            "videos_without_channels",
            "warning",
            f"Found {orphan_count} videos without corresponding channels",
            {"orphan_count": orphan_count}
        )


def run_all_health_checks(conn) -> list[HealthCheck]:
    """Run all health checks."""
    checks = [
        check_orphaned_video_stats(conn),
        check_orphaned_comments(conn),
        check_orphaned_transcripts(conn),
        check_summary_table_accuracy(conn),
        check_videos_without_channels(conn),
    ]
    return checks


# ============================================================================
# REPAIR OPERATIONS
# ============================================================================

def repair_summary_table(conn, dry_run: bool = False) -> RepairResult:
    """
    Reconcile video_comment_summary with actual comment counts.

    This fixes any discrepancies between stored_comment_count and actual counts.
    """
    start = time.perf_counter()
    result = RepairResult(operation="repair_summary_table")

    # Find all mismatches
    mismatches = conn.execute("""
        SELECT
            v.video_id,
            COALESCE(s.stored_comment_count, 0) as summary_count,
            COALESCE(c.actual_count, 0) as actual_count,
            c.last_fetch
        FROM videos v
        LEFT JOIN video_comment_summary s ON v.video_id = s.video_id
        LEFT JOIN (
            SELECT video_id, COUNT(*) as actual_count, MAX(fetched_at) as last_fetch
            FROM comments
            GROUP BY video_id
        ) c ON v.video_id = c.video_id
        WHERE COALESCE(s.stored_comment_count, -1) != COALESCE(c.actual_count, 0)
           OR (c.actual_count > 0 AND s.video_id IS NULL)
    """).fetchall()

    result.records_checked = len(mismatches)

    if dry_run:
        result.records_fixed = len(mismatches)
        log.info(f"[DRY RUN] Would fix {len(mismatches)} summary records")
    else:
        fixed = 0
        for row in mismatches:
            video_id, summary_count, actual_count, last_fetch = row
            try:
                conn.execute("""
                    INSERT INTO video_comment_summary
                        (video_id, stored_comment_count, last_comment_fetch)
                    VALUES (?, ?, ?)
                    ON CONFLICT(video_id) DO UPDATE SET
                        stored_comment_count = excluded.stored_comment_count,
                        last_comment_fetch = COALESCE(excluded.last_comment_fetch,
                                                      video_comment_summary.last_comment_fetch)
                """, (video_id, actual_count, last_fetch))
                fixed += 1
            except Exception as e:
                result.errors.append(f"Failed to fix {video_id}: {e}")

        conn.commit()
        result.records_fixed = fixed
        log.info(f"Fixed {fixed} summary records")

    result.duration_ms = (time.perf_counter() - start) * 1000
    return result


def repair_summary_stats_fields(conn, dry_run: bool = False) -> RepairResult:
    """
    Reconcile video_comment_summary stats fields with actual video_stats.

    Ensures latest_youtube_comment_count and last_stats_fetch are accurate.
    """
    start = time.perf_counter()
    result = RepairResult(operation="repair_summary_stats_fields")

    # Find videos where summary stats don't match latest video_stats
    mismatches = conn.execute("""
        SELECT
            v.video_id,
            s.latest_youtube_comment_count as summary_yt_count,
            s.last_stats_fetch as summary_stats_fetch,
            vs.comment_count as actual_yt_count,
            vs.fetched_at as actual_stats_fetch
        FROM videos v
        LEFT JOIN video_comment_summary s ON v.video_id = s.video_id
        LEFT JOIN (
            SELECT vs1.video_id, vs1.comment_count, vs1.fetched_at
            FROM video_stats vs1
            INNER JOIN (
                SELECT video_id, MAX(fetched_at) as max_fetch
                FROM video_stats
                GROUP BY video_id
            ) latest ON vs1.video_id = latest.video_id AND vs1.fetched_at = latest.max_fetch
        ) vs ON v.video_id = vs.video_id
        WHERE vs.comment_count IS NOT NULL
          AND (s.latest_youtube_comment_count IS NULL
               OR s.latest_youtube_comment_count != vs.comment_count
               OR s.last_stats_fetch IS NULL
               OR s.last_stats_fetch != vs.fetched_at)
    """).fetchall()

    result.records_checked = len(mismatches)

    if dry_run:
        result.records_fixed = len(mismatches)
        log.info(f"[DRY RUN] Would fix {len(mismatches)} summary stats fields")
    else:
        fixed = 0
        for row in mismatches:
            video_id, _, _, actual_yt_count, actual_stats_fetch = row
            try:
                conn.execute("""
                    INSERT INTO video_comment_summary
                        (video_id, latest_youtube_comment_count, last_stats_fetch)
                    VALUES (?, ?, ?)
                    ON CONFLICT(video_id) DO UPDATE SET
                        latest_youtube_comment_count = excluded.latest_youtube_comment_count,
                        last_stats_fetch = excluded.last_stats_fetch
                """, (video_id, actual_yt_count, actual_stats_fetch))
                fixed += 1
            except Exception as e:
                result.errors.append(f"Failed to fix stats for {video_id}: {e}")

        conn.commit()
        result.records_fixed = fixed
        log.info(f"Fixed {fixed} summary stats fields")

    result.duration_ms = (time.perf_counter() - start) * 1000
    return result


def run_all_repairs(conn, dry_run: bool = False) -> list[RepairResult]:
    """Run all repair operations."""
    repairs = [
        repair_summary_table(conn, dry_run),
        repair_summary_stats_fields(conn, dry_run),
    ]
    return repairs


# ============================================================================
# COMPLETENESS ANALYSIS
# ============================================================================

def get_channel_completeness(conn) -> list[ChannelCompleteness]:
    """Get completeness metrics for all channels."""

    # Get per-channel metrics in a single efficient query
    rows = conn.execute("""
        SELECT
            c.channel_id,
            c.title,
            -- Video count
            COUNT(DISTINCT v.video_id) as total_videos,
            -- Stats coverage
            COUNT(DISTINCT vs.video_id) as videos_with_stats,
            -- Comment coverage
            COUNT(DISTINCT CASE WHEN cm.comment_count > 0 THEN cm.video_id END) as videos_with_comments,
            COALESCE(SUM(cm.comment_count), 0) as total_comments_stored,
            COALESCE(SUM(vcs.latest_youtube_comment_count), 0) as total_comments_youtube,
            -- Transcript coverage
            COUNT(DISTINCT t.video_id) as videos_with_transcripts
        FROM channels c
        LEFT JOIN videos v ON c.channel_id = v.channel_id
        LEFT JOIN (
            SELECT DISTINCT video_id FROM video_stats
        ) vs ON v.video_id = vs.video_id
        LEFT JOIN (
            SELECT video_id, COUNT(*) as comment_count
            FROM comments
            GROUP BY video_id
        ) cm ON v.video_id = cm.video_id
        LEFT JOIN video_comment_summary vcs ON v.video_id = vcs.video_id
        LEFT JOIN transcripts t ON v.video_id = t.video_id
        GROUP BY c.channel_id, c.title
        ORDER BY total_videos DESC
    """).fetchall()

    results = []
    for row in rows:
        (channel_id, title, total_videos, videos_with_stats,
         videos_with_comments, total_comments_stored, total_comments_youtube,
         videos_with_transcripts) = row

        # Calculate percentages
        stats_pct = (videos_with_stats / total_videos * 100) if total_videos > 0 else 0
        comment_pct = (videos_with_comments / total_videos * 100) if total_videos > 0 else 0
        transcript_pct = (videos_with_transcripts / total_videos * 100) if total_videos > 0 else 0

        results.append(ChannelCompleteness(
            channel_id=channel_id,
            channel_title=title or "Unknown",
            total_videos=total_videos,
            videos_with_stats=videos_with_stats,
            stats_coverage_pct=round(stats_pct, 1),
            videos_with_comments=videos_with_comments,
            total_comments_stored=total_comments_stored,
            total_comments_youtube=total_comments_youtube,
            comment_coverage_pct=round(comment_pct, 1),
            videos_with_transcripts=videos_with_transcripts,
            transcript_coverage_pct=round(transcript_pct, 1),
        ))

    return results


def get_summary_stats(conn) -> dict:
    """Get overall database summary statistics."""
    result = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM channels) as total_channels,
            (SELECT COUNT(*) FROM videos) as total_videos,
            (SELECT COUNT(*) FROM video_stats) as total_stats_records,
            (SELECT COUNT(*) FROM comments) as total_comments,
            (SELECT COUNT(*) FROM transcripts) as total_transcripts,
            (SELECT COUNT(*) FROM video_comment_summary) as summary_records
    """).fetchone()

    return {
        "total_channels": result[0],
        "total_videos": result[1],
        "total_stats_records": result[2],
        "total_comments": result[3],
        "total_transcripts": result[4],
        "summary_records": result[5],
    }


# ============================================================================
# REPORTING
# ============================================================================

def print_health_checks(checks: list[HealthCheck]):
    """Print health check results."""
    print("\n" + "=" * 60)
    print("HEALTH CHECKS")
    print("=" * 60)

    for check in checks:
        icon = {"ok": "[OK]", "warning": "[WARN]", "error": "[ERR]"}.get(check.status, "[?]")
        print(f"{icon} {check.check_name}: {check.message}")
        if check.details:
            for key, value in check.details.items():
                print(f"     {key}: {value}")


def print_repairs(repairs: list[RepairResult]):
    """Print repair results."""
    print("\n" + "=" * 60)
    print("REPAIR OPERATIONS")
    print("=" * 60)

    for repair in repairs:
        print(f"\n{repair.operation}:")
        print(f"  Checked: {repair.records_checked}")
        print(f"  Fixed:   {repair.records_fixed}")
        print(f"  Time:    {repair.duration_ms:.1f}ms")
        if repair.errors:
            print(f"  Errors:  {len(repair.errors)}")
            for err in repair.errors[:5]:
                print(f"    - {err}")


def print_completeness(channels: list[ChannelCompleteness]):
    """Print completeness analysis."""
    print("\n" + "=" * 60)
    print("CHANNEL COMPLETENESS")
    print("=" * 60)

    # Header
    print(f"\n{'Channel':<30} {'Videos':>7} {'Stats%':>7} {'Comments%':>9} {'Transcripts%':>12}")
    print("-" * 70)

    for ch in channels:
        title = ch.channel_title[:28] + ".." if len(ch.channel_title) > 30 else ch.channel_title
        print(f"{title:<30} {ch.total_videos:>7} {ch.stats_coverage_pct:>6.1f}% "
              f"{ch.comment_coverage_pct:>8.1f}% {ch.transcript_coverage_pct:>11.1f}%")

    # Summary
    if channels:
        total_videos = sum(c.total_videos for c in channels)
        avg_stats = sum(c.stats_coverage_pct for c in channels) / len(channels)
        avg_comments = sum(c.comment_coverage_pct for c in channels) / len(channels)
        avg_transcripts = sum(c.transcript_coverage_pct for c in channels) / len(channels)

        print("-" * 70)
        print(f"{'TOTAL/AVG':<30} {total_videos:>7} {avg_stats:>6.1f}% "
              f"{avg_comments:>8.1f}% {avg_transcripts:>11.1f}%")


def print_summary(summary: dict):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("DATABASE SUMMARY")
    print("=" * 60)

    for key, value in summary.items():
        label = key.replace("_", " ").title()
        print(f"  {label}: {value:,}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Database maintenance and completeness analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--check", action="store_true", help="Run health checks")
    parser.add_argument("--repair", action="store_true", help="Repair inconsistencies")
    parser.add_argument("--completeness", action="store_true", help="Show completeness analysis")
    parser.add_argument("--all", action="store_true", help="Run all operations")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify data")
    parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    # Default to --all if no specific operation requested
    if not any([args.check, args.repair, args.completeness, args.all]):
        args.all = True

    if args.all:
        args.check = args.repair = args.completeness = True

    # Connect to database
    log.info("Connecting to database...")
    conn = get_connection()
    init_database(conn)

    start_time = time.perf_counter()
    report = MaintenanceReport(timestamp=datetime.now().isoformat())

    # Run operations
    if args.check:
        log.info("Running health checks...")
        report.health_checks = run_all_health_checks(conn)

    if args.repair:
        log.info("Running repairs..." + (" (dry run)" if args.dry_run else ""))
        report.repairs = run_all_repairs(conn, args.dry_run)

    if args.completeness:
        log.info("Analyzing completeness...")
        report.channel_completeness = get_channel_completeness(conn)
        report.summary = get_summary_stats(conn)

    report.duration_ms = (time.perf_counter() - start_time) * 1000

    # Output
    if args.json:
        # Convert dataclasses to dicts
        output = {
            "timestamp": report.timestamp,
            "duration_ms": report.duration_ms,
            "health_checks": [asdict(c) for c in report.health_checks],
            "repairs": [asdict(r) for r in report.repairs],
            "channel_completeness": [asdict(c) for c in report.channel_completeness],
            "summary": report.summary,
        }
        print(json.dumps(output, indent=2))
    else:
        if args.check:
            print_health_checks(report.health_checks)
        if args.repair:
            print_repairs(report.repairs)
        if args.completeness:
            print_summary(report.summary)
            print_completeness(report.channel_completeness)

        print(f"\nCompleted in {report.duration_ms:.1f}ms")

    # Exit with error if any health checks failed
    errors = [c for c in report.health_checks if c.status == "error"]
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
