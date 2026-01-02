#!/usr/bin/env python3
"""
YouTube Channel Metadata Fetcher

Fetches and stores YouTube channel metadata in DuckDB for ongoing analysis.
Supports incremental updates, multiple channels, and efficient storage.

Usage:
    # Fetch single channel
    python fetch.py --channel @GoogleDevelopers
    
    # Fetch all channels from config
    python fetch.py --config config/channels.yaml
    
    # Backfill mode (fetch everything, ignore incremental)
    python fetch.py --channel @GoogleDevelopers --backfill
    
    # Export to Parquet
    python fetch.py --export
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml

from database import (
    get_connection,
    init_database,
    get_last_fetch_time,
    get_existing_video_ids,
    get_videos_without_transcripts,
    get_latest_comment_time,
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
    export_to_parquet,
)
from youtube_api import YouTubeFetcher, TranscriptFetcher


def load_config(config_path: str) -> dict:
    """Load channel configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def fetch_channel_data(
    fetcher: YouTubeFetcher,
    conn,
    channel_identifier: str,
    fetch_comments: bool = True,
    fetch_transcripts: bool = True,
    max_videos: int = None,
    max_comments_per_video: int = 500,
    backfill: bool = False,
    verbose: bool = True
) -> dict:
    """
    Fetch all data for a single channel.
    
    Returns dict with counts of fetched items.
    """
    def log(msg):
        if verbose:
            print(msg)
    
    # Resolve channel ID
    log(f"\n{'='*60}")
    log(f"Processing: {channel_identifier}")
    log(f"{'='*60}")
    
    channel_id = fetcher.resolve_channel_id(channel_identifier)
    log(f"Resolved channel ID: {channel_id}")
    
    # Determine fetch type
    last_fetch = get_last_fetch_time(conn, channel_id)
    fetch_type = "backfill" if backfill or not last_fetch else "incremental"
    log(f"Fetch type: {fetch_type}")
    if last_fetch:
        log(f"Last fetch: {last_fetch}")
    
    # Start fetch log
    fetch_id = start_fetch_log(conn, channel_id, fetch_type)
    
    stats = {
        "videos_fetched": 0,
        "videos_new": 0,
        "comments_fetched": 0,
        "transcripts_fetched": 0,
        "errors": []
    }
    
    try:
        # Fetch channel metadata
        log("\nFetching channel metadata...")
        channel_data = fetcher.fetch_channel(channel_id)
        upsert_channel(conn, channel_data)
        insert_channel_stats(conn, channel_id, channel_data["statistics"])
        log(f"  Channel: {channel_data['title']}")
        log(f"  Subscribers: {channel_data['statistics']['subscriber_count']:,}")
        log(f"  Total videos: {channel_data['statistics']['video_count']:,}")
        
        # Fetch playlists
        log("\nFetching playlists...")
        playlists = fetcher.fetch_playlists(channel_id)
        for playlist in playlists:
            upsert_playlist(conn, playlist)
        log(f"  Found {len(playlists)} playlists")
        
        # Get existing video IDs for incremental
        existing_video_ids = get_existing_video_ids(conn, channel_id)
        log(f"  Existing videos in DB: {len(existing_video_ids)}")
        
        # Fetch video IDs from uploads playlist
        uploads_playlist = channel_data.get("uploads_playlist_id")
        if not uploads_playlist:
            raise ValueError("Could not find uploads playlist")
        
        log("\nFetching video list...")
        all_video_ids = fetcher.fetch_playlist_video_ids(uploads_playlist, max_results=max_videos)
        log(f"  Found {len(all_video_ids)} videos in uploads playlist")
        
        # Determine which videos to fetch
        if backfill:
            video_ids_to_fetch = all_video_ids
        else:
            # For incremental, fetch all videos but we'll only get new ones' full data
            # We still need stats for existing videos
            video_ids_to_fetch = all_video_ids
        
        new_video_ids = set(all_video_ids) - existing_video_ids
        log(f"  New videos: {len(new_video_ids)}")
        
        # Fetch video metadata in batches
        log("\nFetching video metadata...")
        videos = fetcher.fetch_videos(video_ids_to_fetch)
        stats["videos_fetched"] = len(videos)
        
        for video in videos:
            video_id = video["video_id"]
            is_new = video_id in new_video_ids
            
            # Always update video metadata and stats
            upsert_video(conn, video)
            insert_video_stats(conn, video_id, video["statistics"])
            
            # Update chapters
            if video.get("chapters"):
                upsert_chapters(conn, video_id, video["chapters"])
            
            if is_new:
                stats["videos_new"] += 1
        
        log(f"  Processed {len(videos)} videos ({stats['videos_new']} new)")
        
        # Fetch transcripts (only for videos without transcripts)
        if fetch_transcripts:
            log("\nFetching transcripts...")
            videos_needing_transcripts = get_videos_without_transcripts(conn, channel_id)
            log(f"  Videos needing transcripts: {len(videos_needing_transcripts)}")
            
            for i, video_id in enumerate(videos_needing_transcripts):
                try:
                    transcript = TranscriptFetcher.fetch(video_id)
                    if transcript and transcript.get("available"):
                        insert_transcript(conn, video_id, transcript)
                        stats["transcripts_fetched"] += 1
                    
                    if (i + 1) % 10 == 0:
                        log(f"    Progress: {i + 1}/{len(videos_needing_transcripts)}")
                except Exception as e:
                    stats["errors"].append(f"Transcript {video_id}: {str(e)}")
            
            log(f"  Fetched {stats['transcripts_fetched']} transcripts")
        
        # Fetch comments
        if fetch_comments:
            log("\nFetching comments...")
            total_new_comments = 0
            
            # For incremental, only fetch new comments
            for i, video in enumerate(videos):
                video_id = video["video_id"]
                
                # Get cutoff time for incremental
                if not backfill:
                    since = get_latest_comment_time(conn, video_id)
                else:
                    since = None
                
                try:
                    comments = fetcher.fetch_comments(
                        video_id, 
                        since=since,
                        max_results=max_comments_per_video
                    )
                    
                    if comments:
                        new_count = insert_comments(conn, comments)
                        total_new_comments += new_count
                        stats["comments_fetched"] += len(comments)
                    
                    if (i + 1) % 20 == 0:
                        log(f"    Progress: {i + 1}/{len(videos)} videos, {total_new_comments} new comments")
                        
                except Exception as e:
                    stats["errors"].append(f"Comments {video_id}: {str(e)}")
            
            log(f"  Fetched {stats['comments_fetched']} comments ({total_new_comments} new)")
        
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
        
        log(f"\n✅ Completed: {channel_data['title']}")
        
    except Exception as e:
        complete_fetch_log(conn, fetch_id, 0, 0, 0, status="failed", errors=str(e))
        log(f"\n❌ Failed: {str(e)}")
        raise
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Fetch YouTube channel metadata into DuckDB",
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
        help="Export database to Parquet files"
    )
    
    # Database options
    parser.add_argument(
        "--db",
        default="data/youtube.duckdb",
        help="Path to DuckDB database (default: data/youtube.duckdb)"
    )
    
    # Fetch options
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Full backfill mode (ignore incremental, fetch everything)"
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        help="Maximum videos to fetch per channel"
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=500,
        help="Maximum comments per video (default: 500)"
    )
    parser.add_argument(
        "--skip-comments",
        action="store_true",
        help="Skip fetching comments"
    )
    parser.add_argument(
        "--skip-transcripts",
        action="store_true",
        help="Skip fetching transcripts"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Minimal output"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    conn = get_connection(args.db)
    init_database(conn)
    
    # Export mode
    if args.export:
        print("Exporting to Parquet...")
        exported = export_to_parquet(conn)
        for table, path in exported.items():
            print(f"  {table}: {path}")
        print("✅ Export complete")
        return
    
    # Determine channels to fetch
    channels = []
    if args.channel:
        channels = [{"identifier": args.channel}]
    elif args.config:
        config = load_config(args.config)
        channels = config.get("channels", [])
    else:
        parser.print_help()
        print("\nError: Must specify --channel, --config, or --export")
        sys.exit(1)
    
    if not channels:
        print("No channels to fetch")
        sys.exit(1)
    
    # Initialize fetcher
    fetcher = YouTubeFetcher()
    
    # Process each channel
    total_stats = {
        "channels": 0,
        "videos": 0,
        "comments": 0,
        "transcripts": 0,
        "errors": 0
    }
    
    for channel_config in channels:
        if isinstance(channel_config, str):
            identifier = channel_config
            channel_options = {}
        else:
            identifier = channel_config.get("identifier") or channel_config.get("id") or channel_config.get("handle")
            channel_options = channel_config
        
        try:
            stats = fetch_channel_data(
                fetcher=fetcher,
                conn=conn,
                channel_identifier=identifier,
                fetch_comments=not args.skip_comments and channel_options.get("fetch_comments", True),
                fetch_transcripts=not args.skip_transcripts and channel_options.get("fetch_transcripts", True),
                max_videos=args.max_videos or channel_options.get("max_videos"),
                max_comments_per_video=args.max_comments,
                backfill=args.backfill,
                verbose=not args.quiet
            )
            
            total_stats["channels"] += 1
            total_stats["videos"] += stats["videos_fetched"]
            total_stats["comments"] += stats["comments_fetched"]
            total_stats["transcripts"] += stats["transcripts_fetched"]
            total_stats["errors"] += len(stats["errors"])
            
        except Exception as e:
            print(f"Error processing {identifier}: {e}", file=sys.stderr)
            total_stats["errors"] += 1
    
    # Summary
    print(f"\n{'='*60}")
    print("FETCH SUMMARY")
    print(f"{'='*60}")
    print(f"Channels processed: {total_stats['channels']}")
    print(f"Videos fetched: {total_stats['videos']}")
    print(f"Comments fetched: {total_stats['comments']}")
    print(f"Transcripts fetched: {total_stats['transcripts']}")
    print(f"Errors: {total_stats['errors']}")
    
    # Set GitHub Actions outputs
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"channels={total_stats['channels']}\n")
            f.write(f"videos={total_stats['videos']}\n")
            f.write(f"comments={total_stats['comments']}\n")
            f.write(f"transcripts={total_stats['transcripts']}\n")


if __name__ == "__main__":
    main()