#!/usr/bin/env python3
"""
YouTube Analytics Query Tool

Pre-built queries for analyzing YouTube channel data.
Can also run custom SQL queries against the database.
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


def run_query(conn, query: str) -> list:
    """Run a SQL query and return results."""
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
        headers = get_column_names(conn, query)
        rows = run_query(conn, query)
        
        if args.output:
            export_csv(headers, rows, args.output)
        else:
            print_table(headers, rows)
            print(f"\n({len(rows)} rows)")
            
    except Exception as e:
        print(f"Query error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
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


def run_query(conn, query: str) -> list:
    """Run a SQL query and return results."""
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
        headers = get_column_names(conn, query)
        rows = run_query(conn, query)
        
        if args.output:
            export_csv(headers, rows, args.output)
        else:
            print_table(headers, rows)
            print(f"\n({len(rows)} rows)")
            
    except Exception as e:
        print(f"Query error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
