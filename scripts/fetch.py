#!/usr/bin/env python3
"""
YouTube Channel Metadata Fetcher

Fetches and stores YouTube channel metadata in Turso/SQLite for ongoing analysis.
Features:
- Smart incremental updates to minimize redundant API requests
- Quota tracking to prevent exceeding daily limits
- Checkpointing for resumable operations
- Parallel comment fetching for faster processing
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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import yaml

# Initialize config first so logging can use it
from config import get_config

# Initialize logging with config
from logger import setup_logging, get_logger, LogContext, set_channel_context, clear_channel_context

# Set up logging before other imports (will use config values)
logger = setup_logging()
log = get_logger("fetch")

from database import (
    get_connection,
    init_database,
    is_postgres,
    get_last_fetch_time,
    get_latest_video_publish_date,
    get_existing_video_ids,
    get_videos_needing_stats_update,
    get_videos_without_transcripts,
    get_latest_comment_time,
    get_videos_needing_comments,
    should_update_channel_stats,
    should_update_playlists,
    start_fetch_log,
    complete_fetch_log,
    upsert_channel,
    insert_channel_stats,
    upsert_video,
    upsert_videos_batch,
    insert_video_stats,
    insert_video_stats_batch,
    upsert_chapters,
    upsert_chapters_batch,
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
from googleapiclient.errors import HttpError
from youtube_api import YouTubeFetcher
from quota import QuotaTracker, QuotaExhaustedError


# Configuration helper functions (values loaded from config module)
def get_default_batch_size() -> int:
    """Get default batch size from config."""
    return get_config().default_batch_size

def get_max_runtime_minutes() -> int:
    """Get max runtime minutes from config."""
    return get_config().max_runtime_minutes

def get_default_comment_workers() -> int:
    """Get default comment workers from config."""
    return get_config().default_comment_workers

def get_progress_log_interval() -> int:
    """Get progress log interval from config."""
    return get_config().progress_log_interval

def get_progress_callback_interval() -> int:
    """Get progress callback interval from config."""
    return get_config().progress_callback_interval


def load_config(config_path: str) -> dict:
    """Load channel configuration from YAML file."""
    log.debug(f"Loading config from {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    log.debug(f"Loaded {len(config.get('channels', []))} channels from config")
    return config


def fetch_comments_parallel(
    api_key: str,
    conn,
    quota: QuotaTracker,
    video_ids: list[str],
    max_comments_per_video: int,
    backfill: bool,
    num_workers: int = None,
    max_replies_per_comment: int = 10,
    progress_callback: Optional[Callable[[int, int, int], None]] = None,
) -> tuple[int, int, list[str]]:
    """
    Fetch comments for multiple videos in parallel.

    Creates a separate API client per thread to avoid SSL/connection issues.

    Args:
        api_key: YouTube API key (each thread creates its own client)
        conn: Database connection
        quota: QuotaTracker instance
        video_ids: List of video IDs to fetch comments for
        max_comments_per_video: Max comments per video
        backfill: If True, fetch all comments; otherwise only new ones
        num_workers: Number of parallel workers (default: 3)
        max_replies_per_comment: Max replies per top-level comment (default: 10)
        progress_callback: Optional callback(processed, total, new_comments)

    Returns:
        Tuple of (total_comments, new_comments, errors)
    """
    # Use config defaults if not specified
    if num_workers is None:
        num_workers = get_default_comment_workers()

    if not video_ids:
        return 0, 0, []

    # Thread-safe counters
    lock = threading.Lock()
    total_comments = 0
    new_comments = 0
    errors = []
    processed = 0
    stop_flag = threading.Event()
    
    # Thread-local storage for per-thread API clients
    thread_local = threading.local()
    
    def get_thread_fetcher():
        """Get or create a YouTubeFetcher for the current thread."""
        if not hasattr(thread_local, 'fetcher'):
            thread_local.fetcher = YouTubeFetcher(api_key=api_key)
        return thread_local.fetcher
    
    # Pre-fetch 'since' times for incremental mode (avoid DB access in threads)
    since_times = {}
    if not backfill:
        for video_id in video_ids:
            since_times[video_id] = get_latest_comment_time(conn, video_id)
    
    def fetch_single_video(video_id: str) -> tuple[str, list, int, str | None]:
        """Fetch comments for a single video. Returns (video_id, comments, quota_used, error)."""
        if stop_flag.is_set():
            return video_id, [], 0, "stopped"

        # Get thread-local fetcher
        fetcher = get_thread_fetcher()
        since = since_times.get(video_id) if not backfill else None

        try:
            comments = fetcher.fetch_comments(
                video_id,
                since=since,
                max_results=max_comments_per_video,
                max_replies_per_comment=max_replies_per_comment
            )
            quota_used = (len(comments) // 100) + 1 if comments else 1
            return video_id, comments, quota_used, None
        except HttpError as e:
            # Handle YouTube API errors specifically
            error_msg = str(e)
            if "commentsDisabled" in error_msg or e.resp.status == 403:
                log.debug(f"Comments disabled for {video_id}")
            else:
                log.warning(f"HTTP error fetching comments for {video_id}: {e}")
            return video_id, [], 1, error_msg
        except (ConnectionError, TimeoutError, OSError) as e:
            # Handle network-related errors
            log.warning(f"Network error fetching comments for {video_id}: {e}")
            return video_id, [], 1, str(e)
        except Exception as e:
            # Catch-all for unexpected errors in thread context
            log.warning(f"Unexpected error fetching comments for {video_id}: {type(e).__name__}: {e}")
            return video_id, [], 1, str(e)
    
    # Process with thread pool
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(fetch_single_video, vid): vid for vid in video_ids}
        
        for future in as_completed(futures):
            video_id = futures[future]
            
            # Check quota before processing result
            with lock:
                if not quota.can_afford('commentThreads.list', 1):
                    log.warning("Insufficient quota, stopping comment fetch")
                    stop_flag.set()
                    break
            
            try:
                vid, comments, quota_used, error = future.result()

                with lock:
                    # Update quota
                    quota.use('commentThreads.list', quota_used)

                    if error and "stopped" not in error:
                        errors.append(f"Comments {vid}: {error}")

                    if comments:
                        # Database write - SQLite handles this safely with WAL mode
                        new_count = insert_comments(conn, comments)
                        total_comments += len(comments)
                        new_comments += new_count
                        log.debug(f"Video {vid}: {len(comments)} comments ({new_count} new)")

                    processed += 1

                    # Progress callback
                    if progress_callback and processed % get_progress_callback_interval() == 0:
                        progress_callback(processed, len(video_ids), new_comments)

            except (KeyboardInterrupt, SystemExit):
                # Re-raise system-level interrupts
                raise
            except Exception as e:
                # Handle any other errors from future execution
                with lock:
                    log.debug(f"Future result error for {video_id}: {type(e).__name__}: {e}")
                    errors.append(f"Comments {video_id}: {str(e)}")
                    processed += 1
    
    return total_comments, new_comments, errors


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
    max_comment_videos: int = 200,
    max_stats_videos: int = 500,
    comment_workers: int = None,
    stats_update_hours: int = 6,
    playlists_update_hours: int = 24,
    comments_refresh_tiers: list[dict] = None,
    stats_refresh_tiers: list[dict] = None,
    video_discovery_mode: str = "auto",
    batch_size: int = None,
    backfill: bool = False,
    start_time: float = None,
    max_runtime_minutes: int = None,
    min_new_comments: int = 0,
) -> dict:
    """
    Fetch all data for a single channel with smart incremental updates.
    
    Features:
    - Batched commits with checkpointing for resume
    - Quota tracking and early abort
    - Parallel comment fetching for faster processing
    - Time limit awareness
    - Tiered refresh rates based on video age
    
    Note: Transcripts are fetched separately via fetch_transcripts.py
    because YouTube blocks requests from cloud/CI environments.
    
    Args:
        batch_size: Number of videos to process before committing (default: 10)
        max_runtime_minutes: Stop if approaching this runtime limit
        max_comments_per_video: Max comment threads per video (default: 500)
        max_comment_videos: Max videos to fetch comments for per run (default: 200)
        max_stats_videos: Max videos to refresh stats for per run (default: 500)
        min_new_comments: Skip videos with fewer than this many new comments (default: 0)
                         Set to 10 to skip low-engagement videos and reduce API calls
        playlists_update_hours: Hours between playlist refresh (default: 24)
        comments_refresh_tiers: List of {'max_age_days': N, 'refresh_hours': M} for comments
                               Default: [{'max_age_days': 2, 'refresh_hours': 6},
                                         {'max_age_days': 7, 'refresh_hours': 12},
                                         {'max_age_days': None, 'refresh_hours': 24}]
        stats_refresh_tiers: List of {'max_age_days': N, 'refresh_hours': M} for video stats
                            Default: same as comments_refresh_tiers
        video_discovery_mode: How to discover new videos:
            - "auto": Use search API for incremental, playlist for backfill (default)
            - "search": Always use search API (100 units/call, stops at known videos)
            - "playlist": Always use playlist API (1 unit/50 videos, fetches all)
    
    Returns:
        Dict with counts of fetched items and status.
    """
    # Use config defaults if not specified
    if comment_workers is None:
        comment_workers = get_default_comment_workers()
    if batch_size is None:
        batch_size = get_default_batch_size()
    if max_runtime_minutes is None:
        max_runtime_minutes = get_max_runtime_minutes()

    # Default refresh tiers
    if comments_refresh_tiers is None:
        comments_refresh_tiers = [
            {'max_age_days': 2, 'refresh_hours': 6},      # < 48h: every 6h
            {'max_age_days': 7, 'refresh_hours': 12},     # 48h-7d: every 12h
            {'max_age_days': 30, 'refresh_hours': 48},    # 7d-30d: every 48h
            {'max_age_days': None, 'refresh_hours': 168}  # 30d+: every 7 days
        ]
    
    if stats_refresh_tiers is None:
        stats_refresh_tiers = [
            {'max_age_days': 2, 'refresh_hours': 0},      # < 48h: every run
            {'max_age_days': 7, 'refresh_hours': 6},      # 48h-7d: every 6h
            {'max_age_days': 30, 'refresh_hours': 12},    # 7d-30d: every 12h
            {'max_age_days': None, 'refresh_hours': 24}   # 30d+: every 24h
        ]
    
    start_time = start_time or time.time()
    channel_start_time = time.time()  # Per-channel timing (start_time is for overall budget)

    def check_time_budget() -> int:
        """Check remaining time, raise if too low."""
        elapsed_minutes = (time.time() - start_time) / 60
        remaining = max_runtime_minutes - elapsed_minutes
        if remaining < 5:
            raise TimeoutError(f"Time limit approaching: only {remaining:.1f} minutes remaining")
        return int(remaining)
    
    # Set channel context for log messages (shows prefix in parallel mode)
    set_channel_context(channel_identifier)

    # Resolve channel ID
    log.info(f"Processing: {channel_identifier}")

    with LogContext(log, f"Resolving channel ID for {channel_identifier}"):
        channel_id = fetcher.resolve_channel_id(channel_identifier)
        quota.use('channels.list')  # Resolution uses API
    
    log.debug(f"Resolved channel ID: {channel_id}")

    # Determine fetch type
    last_fetch = get_last_fetch_time(conn, channel_id)
    fetch_type = "backfill" if backfill or not last_fetch else "incremental"
    log.debug(f"Fetch type: {fetch_type}, last fetch: {last_fetch}")
    
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
        
        # Compact channel info with human-readable subscriber count
        sub_count = channel_data['statistics']['subscriber_count']
        if sub_count >= 1_000_000:
            sub_str = f"{sub_count / 1_000_000:.1f}M"
        elif sub_count >= 1_000:
            sub_str = f"{sub_count / 1_000:.0f}k"
        else:
            sub_str = str(sub_count)
        video_count = channel_data['statistics']['video_count']
        log.info(f"{channel_data['title']} ({sub_str} subs, {video_count} videos)")

        # Estimate quota needed
        estimated_quota = quota.estimate_channel_cost(
            video_count=min(max_videos or 99999, video_count),
            fetch_comments=fetch_comments,
            max_comments_per_video=max_comments_per_video
        )
        log.debug(f"Estimated quota: {estimated_quota['total']} units, remaining: {quota.remaining()}")

        if estimated_quota['total'] > quota.remaining():
            log.warning(f"Insufficient quota for full fetch, will do partial")
        
        # Channel stats
        if backfill or should_update_channel_stats(conn, channel_id, stats_update_hours):
            insert_channel_stats(conn, channel_id, channel_data["statistics"])
            log.debug("Updated channel stats")
        else:
            log.debug(f"Channel stats up-to-date (within {stats_update_hours}h)")
        
        # ================================================================
        # EARLY EXIT: Skip video discovery if video count unchanged
        # ================================================================
        youtube_video_count = channel_data['statistics']['video_count']
        existing_video_ids = get_existing_video_ids(conn, channel_id)
        stored_video_count = len(existing_video_ids)
        
        # For incremental fetches, skip expensive video discovery if counts match
        if not backfill and youtube_video_count == stored_video_count and stored_video_count > 0:
            log.info(f"Video count unchanged ({youtube_video_count}), skipping video discovery")
            
            # Still do comments for videos that need them
            if fetch_comments:
                videos_for_comments = get_videos_needing_comments(
                    conn,
                    channel_id,
                    refresh_tiers=comments_refresh_tiers,
                    limit=max_comment_videos,
                    min_new_comments=min_new_comments
                )
                
                if videos_for_comments:
                    workers = comment_workers if comment_workers > 1 else 1
                    log.info(f"Fetching comments for {len(videos_for_comments)} videos "
                            f"({workers} worker{'s' if workers > 1 else ''})")
                    
                    def progress_callback(processed, total, new_comments):
                        log.info(f"Comment progress: {processed}/{total} videos, {new_comments} new comments")
                    
                    total_fetched, total_new, comment_errors = fetch_comments_parallel(
                        api_key=fetcher.api_key,
                        conn=conn,
                        quota=quota,
                        video_ids=videos_for_comments,
                        max_comments_per_video=max_comments_per_video,
                        backfill=backfill,
                        num_workers=workers,
                        max_replies_per_comment=max_replies_per_comment,
                        progress_callback=progress_callback,
                    )

                    stats["comments_fetched"] += total_fetched
                    stats["errors"].extend(comment_errors)

                    log.info(f"Fetched {total_fetched} comments ({total_new} new)")
                else:
                    log.info("No videos need comment updates")

            # Update fetch log for early completion
            error_str = "; ".join(stats["errors"][:10]) if stats["errors"] else None
            complete_fetch_log(
                conn, fetch_id,
                videos=stats["videos_fetched"],
                comments=stats["comments_fetched"],
                transcripts=stats["transcripts_fetched"],
                status="completed",
                errors=error_str
            )

            # Concise summary and cleanup for early return
            elapsed = time.time() - channel_start_time
            log.info(f"✓ Done in {elapsed:.0f}s: no new videos, {stats['comments_fetched']} comments")
            quota.flush()
            clear_channel_context()
            return stats
        
        # ================================================================
        # PLAYLISTS (throttled)
        # ================================================================
        if backfill or should_update_playlists(conn, channel_id, playlists_update_hours):
            with LogContext(log, "Fetching playlists"):
                playlists = fetcher.fetch_playlists(channel_id)
                quota.use('playlists.list', (len(playlists) // 50) + 1)
                for playlist in playlists:
                    upsert_playlist(conn, playlist)
            log.info(f"Found {len(playlists)} playlists")
        else:
            log.debug(f"Playlists up-to-date (within {playlists_update_hours}h)")
        
        # ================================================================
        # VIDEOS - Smart incremental fetching with checkpointing
        # ================================================================
        # existing_video_ids already fetched above for video count comparison
        log.info(f"Existing videos in DB: {len(existing_video_ids)}")
        
        uploads_playlist = channel_data.get("uploads_playlist_id")
        if not uploads_playlist:
            raise ValueError("Could not find uploads playlist")
        
        # ================================================================
        # VIDEO DISCOVERY - Choose strategy based on mode
        # ================================================================
        # Determine effective discovery mode
        effective_mode = video_discovery_mode
        if effective_mode == "auto":
            # Use search for incremental (cheaper when few new videos)
            # Use playlist for backfill (cheaper when fetching all)
            effective_mode = "playlist" if backfill else "search"
        
        all_video_ids = []
        new_video_ids = set()
        search_api_calls = 0
        
        if effective_mode == "search" and existing_video_ids:
            # Use search API - stops when we hit known videos
            # More efficient when there are only a few new videos
            latest_publish = get_latest_video_publish_date(conn, channel_id)
            
            log.info(f"Using search API for video discovery (latest video: {latest_publish})")
            
            with LogContext(log, "Searching for new videos"):
                new_ids, search_api_calls = fetcher.search_channel_videos(
                    channel_id,
                    published_after=latest_publish,
                    known_video_ids=existing_video_ids,
                    max_results=max_videos,
                )
                quota.use('search.list', search_api_calls)  # 100 units per search call (cost is in COSTS dict)
            
            new_video_ids = set(new_ids)
            # For search mode, we only have new videos - combine with existing for full list
            all_video_ids = new_ids + list(existing_video_ids)
            
            log.info(f"Search found {len(new_video_ids)} new videos ({search_api_calls} API calls, {search_api_calls * 100} quota)")
            
        else:
            # Use playlist API - fetches all video IDs
            # More efficient for backfill or when search isn't appropriate
            log.info(f"Using playlist API for video discovery")
            
            with LogContext(log, "Fetching video list"):
                all_video_ids = fetcher.fetch_playlist_video_ids(uploads_playlist, max_results=max_videos)
                quota.use('playlistItems.list', (len(all_video_ids) // 50) + 1)
            
            log.info(f"Found {len(all_video_ids)} videos in uploads playlist")
            
            # Determine new videos
            current_video_ids = set(all_video_ids)
            new_video_ids = current_video_ids - existing_video_ids
        
        log.info(f"New videos to fetch: {len(new_video_ids)}")
        
        # ================================================================
        # DETECT DELETED VIDEOS (only reliable with playlist mode)
        # ================================================================
        if effective_mode == "playlist":
            current_video_ids = set(all_video_ids)
            deleted_video_ids = existing_video_ids - current_video_ids
            
            if deleted_video_ids:
                log.info(f"Detected {len(deleted_video_ids)} videos no longer on channel")
                deleted_count = mark_videos_as_deleted(conn, list(deleted_video_ids))
                stats["videos_deleted"] = deleted_count
                log.debug(f"Marked {deleted_count} videos as deleted")
        
        # Determine which videos need full metadata fetch
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
        
        # Process videos in batches (YouTube API max is 50 per request)
        if video_ids_to_fetch:
            api_batch_size = 50  # YouTube API maximum
            num_batches = (len(video_ids_to_fetch) + api_batch_size - 1) // api_batch_size
            log.info(f"Fetching metadata for {len(video_ids_to_fetch)} videos ({num_batches} API calls)")
            
            all_stats_to_insert = []
            
            for batch_start in range(0, len(video_ids_to_fetch), api_batch_size):
                # Check time budget
                remaining_mins = check_time_budget()
                log.debug(f"Time remaining: {remaining_mins} minutes")
                
                # Check quota
                if not quota.can_afford('videos.list', 1):
                    log.warning("Insufficient quota for next batch, stopping video fetch")
                    break
                
                batch_ids = video_ids_to_fetch[batch_start:batch_start + api_batch_size]
                batch_num = batch_start // api_batch_size + 1
                
                log.debug(f"Processing batch {batch_num}/{num_batches}: {len(batch_ids)} videos")
                
                try:
                    with LogContext(log, f"Batch {batch_num}/{num_batches}"):
                        videos = fetcher.fetch_videos(batch_ids)
                        quota.use('videos.list', 1)

                        # Filter by age if specified
                        if max_video_age_days:
                            cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_video_age_days)
                            videos = [v for v in videos if _video_is_recent(v, cutoff_date)]

                        # Collect chapters for batch insert
                        chapters_by_video = {}

                        for video in videos:
                            video_id = video["video_id"]
                            all_stats_to_insert.append((video_id, video["statistics"]))

                            if video.get("chapters"):
                                chapters_by_video[video_id] = video["chapters"]

                            processed_ids.add(video_id)
                            stats["videos_fetched"] += 1

                            if video_id in new_video_ids:
                                stats["videos_new"] += 1

                        # Batch DB writes - single network round-trip per operation
                        upsert_videos_batch(conn, videos, commit=False)
                        if chapters_by_video:
                            upsert_chapters_batch(conn, chapters_by_video, commit=False)
                        conn.commit()

                        # Checkpoint: save progress
                        save_progress(conn, channel_id, fetch_id, 'videos',
                                     processed_ids, len(video_ids_to_fetch))
                    
                    # Progress every N batches or at end
                    if batch_num % get_progress_log_interval() == 0 or batch_num == num_batches:
                        log.info(f"Video progress: {batch_num}/{num_batches} batches, "
                                f"{len(processed_ids)}/{len(video_ids_to_fetch)} videos, "
                                f"quota: {quota.remaining()} remaining")
                    
                except Exception as e:
                    log.error(f"Batch {batch_num} failed: {e}")
                    stats["errors"].append(f"Video batch {batch_num}: {str(e)}")
                    # Progress is saved, can resume later
                    raise
            
            # Batch insert all stats at once
            if all_stats_to_insert:
                insert_video_stats_batch(conn, all_stats_to_insert)

            # Checkpoint quota after video phase
            quota.flush()

        # ================================================================
        # UPDATE STATS FOR EXISTING VIDEOS (incremental only)
        # ================================================================
        if not backfill and existing_video_ids:
            videos_needing_stats = get_videos_needing_stats_update(
                conn, 
                channel_id, 
                refresh_tiers=stats_refresh_tiers,
                limit=max_stats_videos
            )
            
            if videos_needing_stats:
                # YouTube API allows 50 videos per request
                api_batch_size = 50
                num_api_calls = (len(videos_needing_stats) + api_batch_size - 1) // api_batch_size
                
                if quota.can_afford('videos.list', num_api_calls):
                    log.info(f"Updating stats for {len(videos_needing_stats)} existing videos")
                    
                    all_stats_to_insert = []
                    
                    for batch_start in range(0, len(videos_needing_stats), api_batch_size):
                        if not quota.can_afford('videos.list', 1):
                            log.warning("Insufficient quota for stats update, stopping")
                            break
                        
                        batch_ids = videos_needing_stats[batch_start:batch_start + api_batch_size]
                        batch_num = batch_start // api_batch_size + 1
                        total_batches = num_api_calls
                        
                        try:
                            existing_videos = fetcher.fetch_videos(batch_ids)
                            quota.use('videos.list', 1)
                            
                            # Collect stats for batch insert
                            for video in existing_videos:
                                all_stats_to_insert.append((video["video_id"], video["statistics"]))
                            
                            # Progress every N batches or at end
                            if batch_num % get_progress_log_interval() == 0 or batch_num == total_batches:
                                log.info(f"Stats progress: {batch_num}/{total_batches} batches, "
                                        f"{len(all_stats_to_insert)} videos fetched")
                                
                        except Exception as e:
                            log.error(f"Stats update batch {batch_num} failed: {e}")
                            stats["errors"].append(f"Stats update: {str(e)}")
                    
                    # Batch insert all stats at once
                    if all_stats_to_insert:
                        log.debug(f"Inserting {len(all_stats_to_insert)} video stats to database")
                        stats["videos_stats_updated"] = insert_video_stats_batch(conn, all_stats_to_insert)
                    
                    log.info(f"Updated stats for {stats['videos_stats_updated']} videos")

                    # Checkpoint quota after stats phase
                    quota.flush()
                else:
                    log.debug(f"Insufficient quota for stats update ({num_api_calls} calls needed)")

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
        # COMMENTS - Only for videos needing updates (parallel)
        # ================================================================
        if fetch_comments:
            if backfill:
                # Even in backfill, limit to most recent videos per run
                videos_for_comments = all_video_ids[:max_comment_videos] if max_comment_videos else all_video_ids
                log.info(f"Backfill: fetching comments for {len(videos_for_comments)} videos (limited to {max_comment_videos})")
            else:
                videos_for_comments = get_videos_needing_comments(
                    conn,
                    channel_id,
                    refresh_tiers=comments_refresh_tiers,
                    limit=max_comment_videos,
                    min_new_comments=min_new_comments
                )
            
            if videos_for_comments:
                # Check for progress (resume from checkpoint)
                comment_progress = get_progress(conn, channel_id, fetch_id, 'comments')
                if comment_progress:
                    processed_comment_video_ids = comment_progress['processed_ids']
                    videos_for_comments = [v for v in videos_for_comments 
                                          if v not in processed_comment_video_ids]
                
                if videos_for_comments:
                    workers = comment_workers if comment_workers > 1 else 1
                    log.info(f"Fetching comments for {len(videos_for_comments)} videos "
                            f"({workers} worker{'s' if workers > 1 else ''})")
                    
                    def progress_callback(processed, total, new_comments):
                        log.info(f"Comment progress: {processed}/{total} videos, {new_comments} new comments")
                    
                    total_fetched, total_new, comment_errors = fetch_comments_parallel(
                        api_key=fetcher.api_key,
                        conn=conn,
                        quota=quota,
                        video_ids=videos_for_comments,
                        max_comments_per_video=max_comments_per_video,
                        backfill=backfill,
                        num_workers=workers,
                        max_replies_per_comment=max_replies_per_comment,
                        progress_callback=progress_callback,
                    )

                    stats["comments_fetched"] += total_fetched
                    stats["errors"].extend(comment_errors)

                    log.info(f"Fetched {total_fetched} comments ({total_new} new)")

                    # Checkpoint quota after comments phase
                    quota.flush()
            else:
                log.debug("All video comments up-to-date")

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
        
        # Concise summary line for parallel processing readability
        elapsed = time.time() - channel_start_time
        log.info(f"✓ Done in {elapsed:.0f}s: {stats['videos_new']} new, "
                 f"{stats['videos_stats_updated']} stats, {stats['comments_fetched']} comments")
        quota.flush()  # Final checkpoint on success

    except QuotaExhaustedError as e:
        log.error(f"✗ Quota exhausted: {e}")
        quota.flush()  # Save quota state on error
        complete_fetch_log(conn, fetch_id,
                          stats["videos_fetched"], stats["comments_fetched"],
                          stats["transcripts_fetched"],
                          status="quota_exhausted", errors=str(e))
        stats["skipped"] = True
        stats["errors"].append(str(e))

    except TimeoutError as e:
        log.error(f"✗ Timeout: {e}")
        quota.flush()  # Save quota state on timeout
        complete_fetch_log(conn, fetch_id,
                          stats["videos_fetched"], stats["comments_fetched"],
                          stats["transcripts_fetched"],
                          status="timeout", errors=str(e))
        stats["errors"].append(str(e))

    except Exception as e:
        log.exception(f"✗ Failed: {e}")
        quota.flush()  # Save quota state on failure
        complete_fetch_log(conn, fetch_id,
                          stats["videos_fetched"], stats["comments_fetched"],
                          stats["transcripts_fetched"],
                          status="failed", errors=str(e))
        raise

    finally:
        # Clear channel context for this thread
        clear_channel_context()

    return stats


def _video_is_recent(video: dict, cutoff_date: datetime) -> bool:
    """
    Check if video was published after the cutoff date.

    Args:
        video: Video dictionary containing 'published_at' field
        cutoff_date: Timezone-aware datetime representing the oldest allowed date

    Returns:
        True if video is recent (published after cutoff) or if date cannot be determined,
        False if video was published before the cutoff date
    """
    published = video.get('published_at')
    if not published:
        return True  # Keep if no date
    try:
        if isinstance(published, str):
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
    videos_needing_comments = get_videos_needing_comments(conn, channel_id, min_new_comments=0) if fetch_comments else []
    
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
        help="Maximum replies per top-level comment (default: 10, prevents blowup on viral comments)"
    )
    parser.add_argument(
        "--max-comment-videos",
        type=int,
        default=200,
        help="Maximum videos to fetch comments for per run (default: 200, prevents extremely long runs)"
    )
    parser.add_argument(
        "--min-new-comments",
        type=int,
        default=10,
        help="Skip videos with fewer than this many new comments since last fetch (default: 10, set to 0 to disable)"
    )
    parser.add_argument(
        "--video-discovery-mode",
        type=str,
        choices=["auto", "search", "playlist"],
        default="auto",
        help="Video discovery strategy: auto (search for incremental, playlist for backfill), "
             "search (100 units/call, stops at known), playlist (1 unit/50 videos, fetches all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Videos to process per batch/commit (default: from config)"
    )
    parser.add_argument(
        "--stats-update-hours",
        type=int,
        default=6,
        help="Only update channel stats if older than this (default: 6 hours)"
    )
    parser.add_argument(
        "--comment-workers",
        type=int,
        default=None,
        help="Parallel workers for comment fetching (default: from config)"
    )
    parser.add_argument(
        "--channel-workers",
        type=int,
        default=None,
        help="Parallel workers for channel processing (default: from config, 1=sequential)"
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=None,
        help="Stop fetching after this many minutes (default: from config)"
    )
    parser.add_argument(
        "--skip-comments",
        action="store_true",
        help="Skip fetching comments"
    )
    parser.add_argument(
        "--quota-limit",
        type=int,
        default=None,
        help="Daily API quota limit (default: from config)"
    )
    parser.add_argument(
        "--reset-quota",
        action="store_true",
        help="Reset today's quota counter to 0 (use if quota tracking was corrupted)"
    )
    
    args = parser.parse_args()
    
    # Resolve config defaults for arguments that weren't specified
    cfg = get_config()
    if args.batch_size is None:
        args.batch_size = cfg.default_batch_size
    if args.comment_workers is None:
        args.comment_workers = cfg.default_comment_workers
    if args.channel_workers is None:
        args.channel_workers = 1  # Default from config settings section
    if args.max_runtime_minutes is None:
        args.max_runtime_minutes = cfg.max_runtime_minutes
    if args.quota_limit is None:
        args.quota_limit = cfg.quota_limit

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
    
    # Initialize quota tracker (creates its own dedicated connection)
    quota = QuotaTracker(daily_limit=args.quota_limit)
    
    # Reset quota if requested
    if args.reset_quota:
        log.info("Resetting quota counter to 0...")
        quota.reset()
        log.info("Quota reset complete")
    
    # Check if quota is already exhausted
    if quota.used >= quota.daily_limit:
        log.error(f"Quota already exhausted: {quota.used}/{quota.daily_limit}")
        log.error("Use --reset-quota if you believe this is incorrect, or wait until quota resets at midnight Pacific time")
        return
    
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
    
    # Process channels (parallel or sequential)
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
    stats_lock = threading.Lock()
    stop_flag = threading.Event()

    def process_single_channel(channel_config):
        """Process a single channel. Used by both sequential and parallel processing."""
        nonlocal total_stats

        # Check if we should stop
        elapsed_minutes = (time.time() - start_time) / 60
        if elapsed_minutes >= args.max_runtime_minutes - 10:  # 10 min buffer
            return "timeout"

        if stop_flag.is_set():
            return "stopped"

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

        # For parallel processing, create dedicated fetcher and connection per thread
        if args.channel_workers > 1:
            thread_fetcher = YouTubeFetcher(api_key)
            thread_conn = get_connection()
        else:
            thread_fetcher = fetcher
            thread_conn = conn

        try:
            stats = fetch_channel_data(
                fetcher=thread_fetcher,
                conn=thread_conn,
                quota=quota,
                channel_identifier=identifier,
                fetch_comments=not args.skip_comments and channel_options.get("fetch_comments", True),
                max_videos=args.max_videos or channel_options.get("max_videos"),
                max_video_age_days=args.max_video_age or channel_options.get("max_video_age_days"),
                max_comments_per_video=get_option("max_comments_per_video", args.max_comments),
                max_replies_per_comment=get_option("max_replies_per_comment", args.max_replies),
                max_comment_videos=get_option("max_comment_videos", args.max_comment_videos),
                min_new_comments=get_option("min_new_comments", args.min_new_comments),
                comment_workers=get_option("comment_workers", args.comment_workers),
                video_discovery_mode=get_option("video_discovery_mode", args.video_discovery_mode),
                batch_size=args.batch_size,
                stats_update_hours=get_option("stats_update_hours", args.stats_update_hours),
                backfill=args.backfill,
                start_time=start_time,
                max_runtime_minutes=args.max_runtime_minutes,
            )

            with stats_lock:
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

            return "success"

        except QuotaExhaustedError:
            log.error(f"Quota exhausted while processing {identifier}")
            stop_flag.set()
            with stats_lock:
                total_stats["channels_skipped"] += 1
            return "quota_exhausted"

        except Exception as e:
            log.exception(f"Error processing {identifier}: {e}")
            with stats_lock:
                total_stats["errors"] += 1
            return "error"

    # Process channels - parallel or sequential
    # Channel workers can be set via CLI or config file (CLI takes precedence)
    global_settings = config.get("settings", {})
    channel_workers = args.channel_workers if args.channel_workers != 1 else global_settings.get("channel_workers", 1)

    # IMPORTANT: Turso's libsql-experimental native library is NOT thread-safe
    # Using parallel workers with Turso causes heap corruption ("malloc(): unsorted double linked list corrupted")
    # Force sequential processing for Turso; PostgreSQL handles parallelization properly
    if channel_workers > 1 and not is_postgres():
        log.warning("Turso backend does not support parallel workers (native library thread-safety issue)")
        log.warning("Forcing sequential processing. Use PostgreSQL backend for parallel channel processing.")
        channel_workers = 1

    if channel_workers > 1:
        log.info(f"Processing {len(channels)} channels with {channel_workers} parallel workers")
        with ThreadPoolExecutor(max_workers=channel_workers) as executor:
            futures = {executor.submit(process_single_channel, ch): ch for ch in channels}

            for future in as_completed(futures):
                result = future.result()
                if result == "timeout":
                    log.warning(f"Approaching time limit, stopping new channel processing")
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break
                elif result == "quota_exhausted":
                    log.error("Quota exhausted, stopping all channel processing")
                    for f in futures:
                        f.cancel()
                    break
    else:
        # Sequential processing (original behavior)
        for channel_config in channels:
            result = process_single_channel(channel_config)
            if result == "timeout":
                log.warning(f"Approaching time limit, stopping")
                break
            elif result == "quota_exhausted":
                break
    
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
