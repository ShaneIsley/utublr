#!/usr/bin/env python3
"""
YouTube Channel Metadata Fetcher

Fetches and stores YouTube channel metadata in Turso/SQLite for ongoing analysis.
Features:
- Smart incremental updates to minimize redundant API requests
- Quota tracking to prevent exceeding daily limits
- Checkpointing for resumable operations
- Detailed DEBUG logging for development

Usage:
    python fetch.py --channel @GoogleDevelopers
    python fetch.py --config config/channels.yaml
    python fetch.py --channel @GoogleDevelopers --backfill
    python fetch.py --export
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import yaml

# Initialize logging first
from logger import setup_logging, get_logger, LogContext

# Set up logging before other imports
logger = setup_logging(log_dir="logs")
log = get_logger("fetch")

from database import (
    get_connection,
    init_database,
    get_last_fetch_time,
    get_existing_video_ids,
    get_videos_needing_stats_update,
    get_videos_without_transcripts,
    get_latest_comment_time,
    get_videos_needing_comments,
    should_update_channel_stats,
    start_fetch_log,
    complete_fetch_log,
    upsert_channel,
    insert_channel_stats,
    upsert_video,
    insert_video_stats,
    upsert_chapters,
    insert_transcript,
    insert_comments,
    upsert_playlist,
    export_to_csv,
    get_progress,
    save_progress,
    clear_progress,
    get_all_video_ids_for_channel,
    mark_videos_as_deleted,
)
from youtube_api import YouTubeFetcher, TranscriptFetcher
from quota import QuotaTracker, QuotaExhaustedError


# Configuration
DEFAULT_BATCH_SIZE = 10  # Small batches for safety with popular videos
MAX_RUNTIME_MINUTES = 300  # 5 hours default


def load_config(config_path: str) -> dict:
    """Load channel configuration from YAML file."""
    log.debug(f"Loading config from {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    log.debug(f"Loaded {len(config.get('channels', []))} channels from config")
    return config


def fetch_channel_data(
    fetcher: YouTubeFetcher,
    conn,
    quota: QuotaTracker,
    channel_identifier: str,
    fetch_comments: bool = True,
    max_videos: int = None,
    max_video_age_days: int = None,
    max_comments_per_video: int = 500,
    max_replies_per_comment: int = 10,
    stats_update_hours: int = 6,
    comments_update_hours: int = 24,
    comments_update_hours_new: int = 6,
    new_video_days: int = 7,
    batch_size: int = DEFAULT_BATCH_SIZE,
    backfill: bool = False,
    start_time: float = None,
    max_runtime_minutes: int = MAX_RUNTIME_MINUTES,
) -> dict:
    """
    Fetch all data for a single channel with smart incremental updates.
    
    Features:
    - Batched commits with checkpointing for resume
    - Quota tracking and early abort
    - Time limit awareness
    - Tiered comment updates (more frequent for new videos)
    
    Note: Transcripts are fetched separately via fetch_transcripts.py
    because YouTube blocks requests from cloud/CI environments.
    
    Args:
        batch_size: Number of videos to process before committing (default: 10)
        max_runtime_minutes: Stop if approaching this runtime limit
        comments_update_hours: Hours between comment re-fetch for older videos (default: 24)
        comments_update_hours_new: Hours between comment re-fetch for new videos (default: 6)
        new_video_days: Videos younger than this are "new" (default: 7)
    
    Returns:
        Dict with counts of fetched items and status.
    """
    start_time = start_time or time.time()
    
    def check_time_budget() -> int:
        """Check remaining time, raise if too low."""
        elapsed_minutes = (time.time() - start_time) / 60
        remaining = max_runtime_minutes - elapsed_minutes
        if remaining < 5:
            raise TimeoutError(f"Time limit approaching: only {remaining:.1f} minutes remaining")
        return int(remaining)
    
    # Resolve channel ID
    log.info(f"{'='*60}")
    log.info(f"Processing: {channel_identifier}")
    log.info(f"{'='*60}")
    
    with LogContext(log, f"Resolving channel ID for {channel_identifier}"):
        channel_id = fetcher.resolve_channel_id(channel_identifier)
        quota.use('channels.list')  # Resolution uses API
    
    log.info(f"Resolved channel ID: {channel_id}")
    
    # Determine fetch type
    last_fetch = get_last_fetch_time(conn, channel_id)
    fetch_type = "backfill" if backfill or not last_fetch else "incremental"
    log.info(f"Fetch type: {fetch_type}")
    if last_fetch:
        log.info(f"Last fetch: {last_fetch}")
    
    # Start fetch log
    fetch_id = start_fetch_log(conn, channel_id, fetch_type)
    log.debug(f"Created fetch_log entry: fetch_id={fetch_id}")
    
    stats = {
        "videos_fetched": 0,
        "videos_new": 0,
        "videos_stats_updated": 0,
        "videos_deleted": 0,
        "comments_fetched": 0,
        "transcripts_fetched": 0,
        "skipped": False,
        "errors": []
    }
    
    try:
        # ================================================================
        # CHANNEL METADATA & STATS
        # ================================================================
        with LogContext(log, "Fetching channel metadata"):
            channel_data = fetcher.fetch_channel(channel_id)
            quota.use('channels.list')
            upsert_channel(conn, channel_data)
        
        log.info(f"Channel: {channel_data['title']}")
        log.info(f"Subscribers: {channel_data['statistics']['subscriber_count']:,}")
        log.info(f"Total videos: {channel_data['statistics']['video_count']:,}")
        
        # Estimate quota needed
        estimated_quota = quota.estimate_channel_cost(
            video_count=min(max_videos or 99999, channel_data['statistics']['video_count']),
            fetch_comments=fetch_comments,
            max_comments_per_video=max_comments_per_video
        )
        log.info(f"Estimated quota needed: {estimated_quota['total']} units")
        log.info(f"Quota remaining: {quota.remaining()} units")
        
        if estimated_quota['total'] > quota.remaining():
            log.warning(f"Insufficient quota for full fetch. Will do partial.")
        
        # Channel stats
        if backfill or should_update_channel_stats(conn, channel_id, stats_update_hours):
            insert_channel_stats(conn, channel_id, channel_data["statistics"])
            log.debug("Updated channel stats")
        else:
            log.debug(f"Channel stats up-to-date (within {stats_update_hours}h)")
        
        # ================================================================
        # PLAYLISTS
        # ================================================================
        with LogContext(log, "Fetching playlists"):
            playlists = fetcher.fetch_playlists(channel_id)
            quota.use('playlists.list', (len(playlists) // 50) + 1)
            for playlist in playlists:
                upsert_playlist(conn, playlist)
        log.info(f"Found {len(playlists)} playlists")
        
        # ================================================================
        # VIDEOS - Smart incremental fetching with checkpointing
        # ================================================================
        existing_video_ids = get_existing_video_ids(conn, channel_id)
        log.info(f"Existing videos in DB: {len(existing_video_ids)}")
        
        uploads_playlist = channel_data.get("uploads_playlist_id")
        if not uploads_playlist:
            raise ValueError("Could not find uploads playlist")
        
        with LogContext(log, "Fetching video list"):
            all_video_ids = fetcher.fetch_playlist_video_ids(uploads_playlist, max_results=max_videos)
            quota.use('playlistItems.list', (len(all_video_ids) // 50) + 1)
        
        log.info(f"Found {len(all_video_ids)} videos in uploads playlist")
        
        # ================================================================
        # DETECT DELETED VIDEOS
        # ================================================================
        current_video_ids = set(all_video_ids)
        deleted_video_ids = existing_video_ids - current_video_ids
        
        if deleted_video_ids:
            log.info(f"Detected {len(deleted_video_ids)} videos no longer on channel")
            deleted_count = mark_videos_as_deleted(conn, list(deleted_video_ids))
            stats["videos_deleted"] = deleted_count
            log.debug(f"Marked {deleted_count} videos as deleted")
        
        # Determine which videos need full metadata fetch
        new_video_ids = current_video_ids - existing_video_ids
        log.info(f"New videos to fetch: {len(new_video_ids)}")
        
        if backfill:
            video_ids_to_fetch = all_video_ids
        else:
            video_ids_to_fetch = list(new_video_ids)
        
        # Check for existing progress (resume support)
        progress = get_progress(conn, channel_id, fetch_id, 'videos')
        if progress:
            processed_ids = progress['processed_ids']
            log.info(f"Resuming: {len(processed_ids)} videos already processed")
            video_ids_to_fetch = [v for v in video_ids_to_fetch if v not in processed_ids]
        else:
            processed_ids = set()
        
        # Process videos in batches
        if video_ids_to_fetch:
            log.info(f"Fetching metadata for {len(video_ids_to_fetch)} videos (batch_size={batch_size})")
            
            for batch_start in range(0, len(video_ids_to_fetch), batch_size):
                # Check time budget
                remaining_mins = check_time_budget()
                log.debug(f"Time remaining: {remaining_mins} minutes")
                
                # Check quota
                if not quota.can_afford('videos.list', 1):
                    log.warning("Insufficient quota for next batch, stopping video fetch")
                    break
                
                batch_ids = video_ids_to_fetch[batch_start:batch_start + batch_size]
                batch_num = batch_start // batch_size + 1
                total_batches = (len(video_ids_to_fetch) + batch_size - 1) // batch_size
                
                log.debug(f"Processing batch {batch_num}/{total_batches}: {len(batch_ids)} videos")
                
                try:
                    with LogContext(log, f"Batch {batch_num}/{total_batches}"):
                        videos = fetcher.fetch_videos(batch_ids)
                        quota.use('videos.list', (len(batch_ids) // 50) + 1)
                        
                        # Filter by age if specified
                        if max_video_age_days:
                            from datetime import timezone
                            cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_video_age_days)
                            videos = [v for v in videos if _video_is_recent(v, cutoff_date)]
                        
                        for video in videos:
                            video_id = video["video_id"]
                            
                            upsert_video(conn, video)
                            insert_video_stats(conn, video_id, video["statistics"])
                            
                            if video.get("chapters"):
                                upsert_chapters(conn, video_id, video["chapters"])
                            
                            processed_ids.add(video_id)
                            stats["videos_fetched"] += 1
                            
                            if video_id in new_video_ids:
                                stats["videos_new"] += 1
                        
                        # Checkpoint: save progress
                        save_progress(conn, channel_id, fetch_id, 'videos', 
                                     processed_ids, len(video_ids_to_fetch))
                    
                    log.info(f"Batch {batch_num}/{total_batches} committed: "
                            f"{len(processed_ids)}/{len(video_ids_to_fetch)} videos, "
                            f"quota: {quota.remaining()} remaining")
                    
                except Exception as e:
                    log.error(f"Batch {batch_num} failed: {e}")
                    stats["errors"].append(f"Video batch {batch_num}: {str(e)}")
                    # Progress is saved, can resume later
                    raise
        
        # ================================================================
        # UPDATE STATS FOR EXISTING VIDEOS (incremental only)
        # ================================================================
        if not backfill and existing_video_ids:
            videos_needing_stats = get_videos_needing_stats_update(conn, channel_id, stats_update_hours)
            
            if videos_needing_stats and quota.can_afford('videos.list', len(videos_needing_stats) // 50 + 1):
                log.info(f"Updating stats for {len(videos_needing_stats)} existing videos")
                
                for batch_start in range(0, len(videos_needing_stats), batch_size):
                    if not quota.can_afford('videos.list', 1):
                        log.warning("Insufficient quota for stats update, stopping")
                        break
                    
                    batch_ids = videos_needing_stats[batch_start:batch_start + batch_size]
                    
                    try:
                        existing_videos = fetcher.fetch_videos(batch_ids)
                        quota.use('videos.list', 1)
                        
                        for video in existing_videos:
                            insert_video_stats(conn, video["video_id"], video["statistics"])
                            stats["videos_stats_updated"] += 1
                    except Exception as e:
                        log.error(f"Stats update batch failed: {e}")
                        stats["errors"].append(f"Stats update: {str(e)}")
                
                log.info(f"Updated stats for {stats['videos_stats_updated']} videos")
            else:
                log.debug(f"Video stats up-to-date or insufficient quota")
        
        # ================================================================
        # TRANSCRIPTS - Skipped (run fetch_transcripts.py locally)
        # ================================================================
        # Note: Transcript fetching is done separately via fetch_transcripts.py
        # because YouTube blocks transcript requests from cloud/CI environments.
        # Run locally: python fetch_transcripts.py --channel CHANNEL_ID
        
        videos_without_transcripts = get_videos_without_transcripts(conn, channel_id)
        if videos_without_transcripts:
            log.info(f"Note: {len(videos_without_transcripts)} videos need transcripts. "
                    f"Run fetch_transcripts.py locally to download them.")
        
        # ================================================================
        # COMMENTS - Only for videos needing updates
        # ================================================================
        if fetch_comments:
            if backfill:
                videos_for_comments = all_video_ids[:max_videos] if max_videos else all_video_ids
                log.info(f"Backfill: fetching comments for all {len(videos_for_comments)} videos")
            else:
                videos_for_comments = get_videos_needing_comments(
                    conn, 
                    channel_id, 
                    hours_since_last=comments_update_hours,
                    hours_since_last_new=comments_update_hours_new,
                    new_video_days=new_video_days
                )
            
            if videos_for_comments:
                log.info(f"Fetching comments for {len(videos_for_comments)} videos")
                
                # Check for progress
                comment_progress = get_progress(conn, channel_id, fetch_id, 'comments')
                if comment_progress:
                    processed_comment_video_ids = comment_progress['processed_ids']
                    videos_for_comments = [v for v in videos_for_comments 
                                          if v not in processed_comment_video_ids]
                else:
                    processed_comment_video_ids = set()
                
                total_new_comments = 0
                
                for i, video_id in enumerate(videos_for_comments):
                    # Check time and quota
                    if i % 5 == 0:
                        check_time_budget()
                    
                    if not quota.can_afford('commentThreads.list', 1):
                        log.warning("Insufficient quota for comments, stopping")
                        break
                    
                    since = None if backfill else get_latest_comment_time(conn, video_id)
                    
                    try:
                        comments = fetcher.fetch_comments(
                            video_id, 
                            since=since,
                            max_results=max_comments_per_video
                        )
                        quota.use('commentThreads.list', (len(comments) // 100) + 1 if comments else 1)
                        
                        if comments:
                            new_count = insert_comments(conn, comments)
                            total_new_comments += new_count
                            stats["comments_fetched"] += len(comments)
                            log.debug(f"Video {video_id}: {len(comments)} comments ({new_count} new)")
                        
                        processed_comment_video_ids.add(video_id)
                        
                        # Checkpoint every 5 videos
                        if (i + 1) % 5 == 0:
                            save_progress(conn, channel_id, fetch_id, 'comments',
                                         processed_comment_video_ids, len(videos_for_comments))
                            log.info(f"Comment progress: {i + 1}/{len(videos_for_comments)} videos, "
                                    f"{total_new_comments} new comments")
                            
                    except Exception as e:
                        log.warning(f"Comment fetch failed for {video_id}: {e}")
                        stats["errors"].append(f"Comments {video_id}: {str(e)}")
                
                log.info(f"Fetched {stats['comments_fetched']} comments ({total_new_comments} new)")
            else:
                log.debug(f"All video comments up-to-date")
        
        # ================================================================
        # CLEANUP & COMPLETION
        # ================================================================
        
        # Clear progress on successful completion
        clear_progress(conn, channel_id, fetch_id)
        
        # Complete fetch log
        error_str = "; ".join(stats["errors"][:10]) if stats["errors"] else None
        complete_fetch_log(
            conn, fetch_id,
            videos=stats["videos_fetched"],
            comments=stats["comments_fetched"],
            transcripts=stats["transcripts_fetched"],
            status="completed",
            errors=error_str
        )
        
        log.info(f"✅ Completed: {channel_data['title']}")
        quota.log_summary()
        
    except QuotaExhaustedError as e:
        log.error(f"Quota exhausted: {e}")
        complete_fetch_log(conn, fetch_id, 
                          stats["videos_fetched"], stats["comments_fetched"], 
                          stats["transcripts_fetched"],
                          status="quota_exhausted", errors=str(e))
        stats["skipped"] = True
        stats["errors"].append(str(e))
        
    except TimeoutError as e:
        log.error(f"Timeout approaching: {e}")
        complete_fetch_log(conn, fetch_id,
                          stats["videos_fetched"], stats["comments_fetched"],
                          stats["transcripts_fetched"],
                          status="timeout", errors=str(e))
        stats["errors"].append(str(e))
        
    except Exception as e:
        log.exception(f"Fetch failed: {e}")
        complete_fetch_log(conn, fetch_id, 
                          stats["videos_fetched"], stats["comments_fetched"],
                          stats["transcripts_fetched"],
                          status="failed", errors=str(e))
        raise
    
    return stats


def _video_is_recent(video: dict, cutoff_date: datetime) -> bool:
    """Check if video was published after cutoff date."""
    published = video.get('published_at')
    if not published:
        return True  # Keep if no date
    try:
        if isinstance(published, str):
            from datetime import timezone
            pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
        else:
            pub_date = published
        return pub_date >= cutoff_date
    except (ValueError, TypeError):
        return True  # Keep if can't parse


def dry_run_channel(
    fetcher: YouTubeFetcher,
    conn,
    quota: QuotaTracker,
    channel_identifier: str,
    max_videos: int = None,
    fetch_comments: bool = True,
) -> dict:
    """
    Preview what would be fetched for a channel without actually fetching.
    Only uses minimal API calls to get counts.
    
    Note: Transcripts are fetched separately via fetch_transcripts.py
    """
    log.info(f"{'='*60}")
    log.info(f"DRY RUN: {channel_identifier}")
    log.info(f"{'='*60}")
    
    # Resolve channel ID (uses 1 API call)
    channel_id = fetcher.resolve_channel_id(channel_identifier)
    quota.use('channels.list')
    
    # Get channel info (uses 1 API call)
    channel_data = fetcher.fetch_channel(channel_id)
    quota.use('channels.list')
    
    log.info(f"Channel: {channel_data['title']}")
    log.info(f"Channel ID: {channel_id}")
    log.info(f"Total videos on YouTube: {channel_data['statistics']['video_count']}")
    
    # Check existing data
    existing_video_ids = get_existing_video_ids(conn, channel_id)
    videos_without_transcripts = get_videos_without_transcripts(conn, channel_id)
    videos_needing_comments = get_videos_needing_comments(conn, channel_id, 24) if fetch_comments else []
    
    log.info(f"")
    log.info(f"CURRENT DATABASE STATE:")
    log.info(f"  Videos in DB: {len(existing_video_ids)}")
    log.info(f"  Videos without transcripts: {len(videos_without_transcripts)} (run fetch_transcripts.py locally)")
    log.info(f"  Videos needing comment update: {len(videos_needing_comments)}")
    
    # Estimate what would be fetched
    total_on_youtube = channel_data['statistics']['video_count']
    videos_to_fetch = max_videos or total_on_youtube
    new_videos_estimate = max(0, videos_to_fetch - len(existing_video_ids))
    
    # Estimate quota
    estimated = quota.estimate_channel_cost(
        video_count=videos_to_fetch,
        fetch_comments=fetch_comments,
        max_comments_per_video=100
    )
    
    log.info(f"")
    log.info(f"ESTIMATED WORK:")
    log.info(f"  Videos to scan: {videos_to_fetch}")
    log.info(f"  New videos (estimate): {new_videos_estimate}")
    log.info(f"  Transcripts to fetch: {len(videos_without_transcripts)}")
    log.info(f"  Comment threads to check: {len(videos_needing_comments)}")
    
    log.info(f"")
    log.info(f"ESTIMATED QUOTA USAGE:")
    for op, cost in estimated.items():
        if op != 'total':
            log.info(f"  {op}: {cost} units")
    log.info(f"  TOTAL: {estimated['total']} units")
    log.info(f"  Quota remaining: {quota.remaining()} units")
    
    can_afford = estimated['total'] <= quota.remaining()
    log.info(f"  Can complete: {'YES' if can_afford else 'NO - insufficient quota'}")
    
    return {
        "channel_id": channel_id,
        "channel_title": channel_data['title'],
        "videos_on_youtube": total_on_youtube,
        "videos_in_db": len(existing_video_ids),
        "new_videos_estimate": new_videos_estimate,
        "transcripts_needed": len(videos_without_transcripts),
        "comments_needed": len(videos_needing_comments),
        "estimated_quota": estimated['total'],
        "quota_remaining": quota.remaining(),
        "can_complete": can_afford,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch YouTube channel metadata into Turso/SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--channel", "-c",
        help="Single channel ID, handle (@username), or URL"
    )
    input_group.add_argument(
        "--config",
        help="Path to channels config YAML file"
    )
    input_group.add_argument(
        "--export",
        action="store_true",
        help="Export database to CSV files"
    )
    
    # Fetch options
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Full backfill mode (ignore incremental, fetch everything)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be fetched without actually fetching"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        help="Maximum videos to fetch per channel (most recent first)"
    )
    parser.add_argument(
        "--max-video-age",
        type=int,
        help="Only fetch videos published within this many days"
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=100,
        help="Maximum comments per video (default: 100)"
    )
    parser.add_argument(
        "--max-replies",
        type=int,
        default=10,
        help="Maximum replies per comment (default: 10, prevents blowup on viral comments)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Videos to process per batch/commit (default: {DEFAULT_BATCH_SIZE})"
    )
    parser.add_argument(
        "--stats-update-hours",
        type=int,
        default=6,
        help="Only update video stats if older than this (default: 6 hours)"
    )
    parser.add_argument(
        "--comments-update-hours",
        type=int,
        default=24,
        help="Hours before re-fetching comments for older videos (default: 24)"
    )
    parser.add_argument(
        "--comments-update-hours-new",
        type=int,
        default=6,
        help="Hours before re-fetching comments for new videos (default: 6)"
    )
    parser.add_argument(
        "--new-video-days",
        type=int,
        default=7,
        help="Videos younger than this many days are 'new' and get more frequent comment updates (default: 7)"
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=MAX_RUNTIME_MINUTES,
        help=f"Stop fetching after this many minutes (default: {MAX_RUNTIME_MINUTES})"
    )
    parser.add_argument(
        "--skip-comments",
        action="store_true",
        help="Skip fetching comments"
    )
    parser.add_argument(
        "--quota-limit",
        type=int,
        default=10000,
        help="Daily API quota limit (default: 10000)"
    )
    
    args = parser.parse_args()
    
    log.info("="*60)
    log.info("YouTube Metadata Fetcher Starting")
    log.info("="*60)
    log.debug(f"Arguments: {vars(args)}")
    
    start_time = time.time()
    
    # Initialize database
    log.info("Connecting to database...")
    conn = get_connection()
    init_database(conn)
    log.info("Database initialized")
    
    # Initialize quota tracker with database connection for persistence
    quota = QuotaTracker(daily_limit=args.quota_limit, conn=conn)
    
    # Export mode
    if args.export:
        log.info("Exporting to CSV...")
        exported = export_to_csv(conn)
        for table, path in exported.items():
            log.info(f"  {table}: {path}")
        log.info("✅ Export complete")
        return
    
    # Determine channels to fetch
    channels = []
    config = {}  # Initialize empty config
    if args.channel:
        channels = [{"identifier": args.channel}]
    elif args.config:
        config = load_config(args.config)
        channels = config.get("channels", [])
    else:
        parser.print_help()
        log.error("Must specify --channel, --config, or --export")
        sys.exit(1)
    
    if not channels:
        log.error("No channels to fetch")
        sys.exit(1)
    
    log.info(f"Processing {len(channels)} channel(s)")
    
    # Initialize fetcher
    fetcher = YouTubeFetcher()
    
    # DRY RUN MODE
    if args.dry_run:
        log.info("=" * 60)
        log.info("DRY RUN MODE - No data will be written")
        log.info("=" * 60)
        
        dry_run_results = []
        for channel_config in channels:
            if isinstance(channel_config, str):
                identifier = channel_config
                channel_options = {}
            else:
                identifier = channel_config.get("identifier") or channel_config.get("id") or channel_config.get("handle")
                channel_options = channel_config
            
            try:
                result = dry_run_channel(
                    fetcher=fetcher,
                    conn=conn,
                    quota=quota,
                    channel_identifier=identifier,
                    max_videos=args.max_videos or channel_options.get("max_videos"),
                    fetch_comments=not args.skip_comments and channel_options.get("fetch_comments", True),
                )
                dry_run_results.append(result)
            except Exception as e:
                log.error(f"Dry run failed for {identifier}: {e}")
        
        # Summary
        log.info("")
        log.info("=" * 60)
        log.info("DRY RUN SUMMARY")
        log.info("=" * 60)
        total_estimated_quota = sum(r['estimated_quota'] for r in dry_run_results)
        log.info(f"Channels analyzed: {len(dry_run_results)}")
        log.info(f"Total estimated quota: {total_estimated_quota}")
        log.info(f"Quota available: {quota.remaining()}")
        
        for r in dry_run_results:
            status = "✓" if r['can_complete'] else "✗"
            log.info(f"  {status} {r['channel_title']}: ~{r['estimated_quota']} units")
        
        return
    
    # Process each channel
    total_stats = {
        "channels": 0,
        "channels_skipped": 0,
        "videos": 0,
        "videos_new": 0,
        "videos_stats_updated": 0,
        "videos_deleted": 0,
        "comments": 0,
        "transcripts": 0,
        "errors": 0
    }
    
    for channel_config in channels:
        # Check if we should stop
        elapsed_minutes = (time.time() - start_time) / 60
        if elapsed_minutes >= args.max_runtime_minutes - 10:  # 10 min buffer
            log.warning(f"Approaching time limit ({elapsed_minutes:.1f} min), stopping")
            break
        
        if isinstance(channel_config, str):
            identifier = channel_config
            channel_options = {}
        else:
            identifier = channel_config.get("identifier") or channel_config.get("id") or channel_config.get("handle")
            channel_options = channel_config
        
        # Get global settings from config, merge with channel-specific options
        global_settings = config.get("settings", {})
        
        # Channel options override global settings
        def get_option(key, default):
            return channel_options.get(key, global_settings.get(key, default))
        
        try:
            stats = fetch_channel_data(
                fetcher=fetcher,
                conn=conn,
                quota=quota,
                channel_identifier=identifier,
                fetch_comments=not args.skip_comments and channel_options.get("fetch_comments", True),
                max_videos=args.max_videos or channel_options.get("max_videos"),
                max_video_age_days=args.max_video_age or channel_options.get("max_video_age_days"),
                max_comments_per_video=get_option("max_comments_per_video", args.max_comments),
                max_replies_per_comment=get_option("max_replies_per_comment", args.max_replies),
                batch_size=args.batch_size,
                stats_update_hours=get_option("stats_update_hours", args.stats_update_hours),
                comments_update_hours=get_option("comments_update_hours", args.comments_update_hours),
                comments_update_hours_new=get_option("comments_update_hours_new", args.comments_update_hours_new),
                new_video_days=get_option("new_video_days", args.new_video_days),
                backfill=args.backfill,
                start_time=start_time,
                max_runtime_minutes=args.max_runtime_minutes,
            )
            
            if stats.get("skipped"):
                total_stats["channels_skipped"] += 1
            else:
                total_stats["channels"] += 1
            
            total_stats["videos"] += stats["videos_fetched"]
            total_stats["videos_new"] += stats["videos_new"]
            total_stats["videos_stats_updated"] += stats.get("videos_stats_updated", 0)
            total_stats["comments"] += stats["comments_fetched"]
            total_stats["transcripts"] += stats.get("transcripts_fetched", 0)
            total_stats["errors"] += len(stats["errors"])
            
        except QuotaExhaustedError:
            log.error(f"Quota exhausted, stopping all fetches")
            total_stats["channels_skipped"] += 1
            break
            
        except Exception as e:
            log.exception(f"Error processing {identifier}: {e}")
            total_stats["errors"] += 1
    
    # Summary
    elapsed = time.time() - start_time
    
    log.info("="*60)
    log.info("FETCH SUMMARY")
    log.info("="*60)
    log.info(f"Runtime: {elapsed/60:.1f} minutes")
    log.info(f"Channels processed: {total_stats['channels']}")
    log.info(f"Channels skipped: {total_stats['channels_skipped']}")
    log.info(f"Videos fetched: {total_stats['videos']} ({total_stats['videos_new']} new)")
    log.info(f"Video stats updated: {total_stats['videos_stats_updated']}")
    log.info(f"Comments fetched: {total_stats['comments']}")
    log.info(f"Transcripts fetched: {total_stats['transcripts']}")
    log.info(f"Errors: {total_stats['errors']}")
    
    quota.log_summary()
    
    # Set GitHub Actions outputs
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"channels={total_stats['channels']}\n")
            f.write(f"videos={total_stats['videos']}\n")
            f.write(f"videos_new={total_stats['videos_new']}\n")
            f.write(f"comments={total_stats['comments']}\n")
            f.write(f"transcripts={total_stats['transcripts']}\n")


if __name__ == "__main__":
    main()
