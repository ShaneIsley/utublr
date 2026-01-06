#!/usr/bin/env python3
"""
Standalone transcript fetching utility.

This script fetches transcripts for videos in the database that don't have them yet.
It must be run locally (not in CI/cloud) because YouTube blocks transcript requests
from cloud IP addresses.

Features:
- Respectful rate limiting (1 request per second by default)
- Retry logic with exponential backoff
- Progress tracking and resumability
- Channel or video-specific fetching
- Detailed failure reason tracking

Usage:
    # Fetch transcripts for all videos without them
    python fetch_transcripts.py
    
    # Fetch for specific channel
    python fetch_transcripts.py --channel @samwitteveenai
    python fetch_transcripts.py --channel UC55ODQSvARtgSyc8ThfiepQ
    
    # Fetch for specific videos
    python fetch_transcripts.py --video VIDEO_ID1 VIDEO_ID2
    
    # Limit number to fetch
    python fetch_transcripts.py --limit 100
    
    # Adjust rate limiting
    python fetch_transcripts.py --delay 2.0  # 2 seconds between requests
    
    # Test mode (don't save to database)
    python fetch_transcripts.py --dry-run --limit 5

Environment Variables:
    TURSO_DATABASE_URL: Database connection URL
    TURSO_AUTH_TOKEN: Database auth token (for Turso cloud)
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logger import get_logger
from database import get_connection, init_database, insert_transcript

log = get_logger("fetch_transcripts")

# ============================================================================
# TRANSCRIPT API SETUP
# ============================================================================

try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import (
        NoTranscriptFound,
        NoTranscriptAvailable,
        TranscriptsDisabled,
        VideoUnavailable,
    )
    TRANSCRIPT_API_AVAILABLE = True
    log.debug("youtube-transcript-api loaded successfully")
except ImportError:
    TRANSCRIPT_API_AVAILABLE = False
    log.error("youtube-transcript-api not installed! Install with: pip install youtube-transcript-api")

# Default rate limiting
DEFAULT_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3
RETRY_DELAY = 5.0  # seconds before retry
PROGRESS_LOG_INTERVAL = 50  # Log progress every N videos

# Singleton API instance
_api_instance = None


def get_transcript_api():
    """Get or create the YouTubeTranscriptApi instance."""
    global _api_instance
    if _api_instance is None and TRANSCRIPT_API_AVAILABLE:
        _api_instance = YouTubeTranscriptApi()
    return _api_instance


# ============================================================================
# TRANSCRIPT FETCHING
# ============================================================================

def fetch_transcript(video_id: str, languages: list = None) -> dict:
    """
    Fetch transcript for a video using youtube-transcript-api v1.x.
    
    Priority:
    1. Manually created transcript in requested languages
    2. Auto-generated transcript in requested languages  
    3. Any transcript translated to English
    
    Args:
        video_id: YouTube video ID
        languages: List of language codes in priority order (default: ['en', 'en-US', 'en-GB'])
    
    Returns:
        Dict with transcript data or availability status
    """
    if languages is None:
        languages = ['en', 'en-US', 'en-GB']
    
    if not TRANSCRIPT_API_AVAILABLE:
        return {"available": False, "reason": "youtube-transcript-api not installed"}
    
    try:
        api = get_transcript_api()
        if api is None:
            return {"available": False, "reason": "Transcript API not available"}
        
        # Get list of available transcripts
        transcript_list = api.list(video_id)
        
        transcript = None
        transcript_info = {}
        
        # Strategy 1: Try to find manually created transcript in preferred languages
        try:
            transcript = transcript_list.find_manually_created_transcript(languages)
            transcript_info = {
                "transcript_type": "manual",
                "language": transcript.language,
                "language_code": transcript.language_code,
                "is_generated": False,
            }
            log.debug(f"Found manual transcript: {transcript.language}")
        except NoTranscriptFound:
            log.debug("No manual transcript in preferred languages")
        
        # Strategy 2: Try auto-generated transcript
        if not transcript:
            try:
                transcript = transcript_list.find_generated_transcript(languages)
                transcript_info = {
                    "transcript_type": "auto-generated",
                    "language": transcript.language,
                    "language_code": transcript.language_code,
                    "is_generated": True,
                }
                log.debug(f"Found auto-generated transcript: {transcript.language}")
            except NoTranscriptFound:
                log.debug("No auto-generated transcript in preferred languages")
        
        # Strategy 3: Translate any available transcript to English
        if not transcript:
            for t in transcript_list:
                if t.is_translatable:
                    try:
                        transcript = t.translate('en')
                        transcript_info = {
                            "transcript_type": "translated",
                            "language": "English (translated)",
                            "language_code": "en",
                            "is_generated": t.is_generated,
                            "original_language": t.language,
                            "original_language_code": t.language_code,
                        }
                        log.debug(f"Translating from {t.language} to English")
                        break
                    except Exception as e:
                        log.debug(f"Translation failed for {t.language}: {e}")
                        continue
        
        if not transcript:
            return {"available": False, "reason": "No transcript in supported languages"}
        
        # Fetch the actual transcript content
        fetched = transcript.fetch()
        
        # Convert to raw data format (list of dicts)
        raw_data = fetched.to_raw_data()
        
        # Build entries with computed end times
        entries = []
        full_text_parts = []
        
        for entry in raw_data:
            entries.append({
                "start": entry["start"],
                "duration": entry["duration"],
                "end": entry["start"] + entry["duration"],
                "text": entry["text"]
            })
            full_text_parts.append(entry["text"])
        
        return {
            "available": True,
            **transcript_info,
            "entries": entries,
            "full_text": " ".join(full_text_parts),
            "snippet_count": len(entries),
        }
        
    except TranscriptsDisabled:
        return {"available": False, "reason": "Transcripts disabled by uploader"}
    except VideoUnavailable:
        return {"available": False, "reason": "Video unavailable"}
    except NoTranscriptAvailable:
        return {"available": False, "reason": "No transcripts available"}
    except Exception as e:
        error_msg = str(e)
        # Check for IP blocking
        if any(x in error_msg.lower() for x in ['blocked', 'ip', '429', 'too many']):
            log.warning(f"Possible IP block for {video_id}: {e}")
            return {"available": False, "reason": f"Request blocked: {error_msg}"}
        log.debug(f"Transcript fetch error for {video_id}: {type(e).__name__}: {e}")
        return {"available": False, "reason": str(e)}


def fetch_transcript_with_retry(
    video_id: str, 
    max_retries: int = MAX_RETRIES,
    retry_delay: float = RETRY_DELAY
) -> dict:
    """
    Fetch transcript with retry logic.
    
    Returns dict with:
        - success: bool
        - transcript: dict (if success)
        - error_type: str (if failure)
        - reason: str
    """
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            transcript = fetch_transcript(video_id)
            
            if transcript and transcript.get("available"):
                return {
                    "success": True,
                    "transcript": transcript,
                    "error_type": None,
                    "reason": f"{transcript.get('transcript_type', 'unknown')} ({transcript.get('language_code', '?')})"
                }
            else:
                # Not available but not an error - don't retry
                reason = transcript.get("reason", "Unknown") if transcript else "Fetch returned None"
                error_type = "disabled" if "disabled" in reason.lower() else "unavailable"
                return {
                    "success": False,
                    "transcript": None,
                    "error_type": error_type,
                    "reason": reason
                }
                
        except Exception as e:
            last_error = e
            error_str = str(e)
            
            # Check if retryable
            retryable = any(x in error_str.lower() for x in [
                "timeout", "connection", "429", "rate limit", "too many"
            ])
            
            if retryable and attempt < max_retries:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                log.warning(f"Retry {attempt + 1}/{max_retries} for {video_id} after {delay:.1f}s: {e}")
                time.sleep(delay)
                continue
            else:
                break
    
    return {
        "success": False,
        "transcript": None,
        "error_type": "error",
        "reason": str(last_error) if last_error else "Unknown error"
    }


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_videos_needing_transcripts(conn, channel_id: str = None, limit: int = None) -> list:
    """
    Get videos that need transcripts.
    
    Args:
        conn: Database connection
        channel_id: Optional channel ID to filter by
        limit: Maximum number of videos to return
    
    Returns:
        List of dicts with video_id and title
    """
    if channel_id:
        query = """
            SELECT v.video_id, v.title, c.title as channel_title
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE v.channel_id = ? AND t.video_id IS NULL
            ORDER BY v.published_at DESC
        """
        params = (channel_id,)
    else:
        query = """
            SELECT v.video_id, v.title, c.title as channel_title
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            LEFT JOIN transcripts t ON v.video_id = t.video_id
            WHERE t.video_id IS NULL
            ORDER BY v.published_at DESC
        """
        params = ()
    
    if limit:
        query += f" LIMIT {limit}"
    
    result = conn.execute(query, params).fetchall()
    return [{"video_id": row[0], "title": row[1], "channel": row[2]} for row in result]


def resolve_channel_id(conn, identifier: str) -> Optional[str]:
    """
    Resolve a channel identifier to a channel ID.
    Checks the database first.
    """
    # Try direct channel ID lookup
    result = conn.execute(
        "SELECT channel_id FROM channels WHERE channel_id = ?",
        (identifier,)
    ).fetchone()
    if result:
        return result[0]
    
    # Try handle lookup (stored in custom_url typically)
    handle = identifier.lstrip("@")
    result = conn.execute(
        "SELECT channel_id FROM channels WHERE custom_url LIKE ? OR title LIKE ?",
        (f"%{handle}%", f"%{handle}%")
    ).fetchone()
    if result:
        return result[0]
    
    return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fetch YouTube transcripts for videos in the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fetch all missing transcripts
    python fetch_transcripts.py
    
    # Fetch for specific channel
    python fetch_transcripts.py --channel @samwitteveenai
    
    # Test with a few videos
    python fetch_transcripts.py --limit 10 --dry-run
    
    # Slower rate limiting for safety
    python fetch_transcripts.py --delay 2.0
        """
    )
    
    parser.add_argument(
        "--channel", "-c",
        help="Channel ID or @handle to fetch transcripts for"
    )
    parser.add_argument(
        "--video", "-v",
        nargs="+",
        help="Specific video ID(s) to fetch"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        help="Maximum number of videos to process"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test mode - fetch transcripts but don't save to database"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Only show summary, not per-video progress"
    )
    
    args = parser.parse_args()
    
    # Check if transcript API is available
    if not TRANSCRIPT_API_AVAILABLE:
        log.error("youtube-transcript-api is not installed!")
        log.error("Install with: pip install youtube-transcript-api")
        sys.exit(1)
    
    # Connect to database
    log.info("Connecting to database...")
    conn = get_connection()
    init_database(conn)
    
    # Determine videos to process
    videos_to_process = []
    
    if args.video:
        # Specific videos
        for video_id in args.video:
            # Check if video exists in database
            result = conn.execute(
                "SELECT title FROM videos WHERE video_id = ?",
                (video_id,)
            ).fetchone()
            if result:
                videos_to_process.append({
                    "video_id": video_id,
                    "title": result[0],
                    "channel": "Unknown"
                })
            else:
                log.warning(f"Video {video_id} not found in database, skipping")
    else:
        # Get from database
        channel_id = None
        if args.channel:
            channel_id = resolve_channel_id(conn, args.channel)
            if not channel_id:
                log.error(f"Channel not found in database: {args.channel}")
                log.error("Make sure the channel has been fetched first with fetch.py")
                sys.exit(1)
            log.info(f"Filtering by channel: {channel_id}")
        
        videos_to_process = get_videos_needing_transcripts(conn, channel_id, args.limit)
    
    if not videos_to_process:
        log.info("No videos need transcripts!")
        return
    
    log.info(f"Found {len(videos_to_process)} videos needing transcripts")
    
    if args.dry_run:
        log.info("DRY RUN MODE - transcripts will not be saved to database")
    
    # Process videos
    stats = {
        "total": len(videos_to_process),
        "success": 0,
        "disabled": 0,
        "unavailable": 0,
        "error": 0,
    }
    
    start_time = time.time()
    
    for i, video in enumerate(videos_to_process):
        video_id = video["video_id"]
        title = video["title"][:40] if video["title"] else "Unknown"
        
        if not args.quiet:
            log.info(f"[{i+1}/{stats['total']}] {video_id}: {title}...")
        
        # Fetch transcript
        result = fetch_transcript_with_retry(video_id)
        
        if result["success"]:
            stats["success"] += 1
            if not args.quiet:
                log.info(f"  ✓ Found: {result['reason']}")
            
            # Save to database
            if not args.dry_run:
                try:
                    insert_transcript(conn, video_id, result["transcript"])
                except Exception as e:
                    log.error(f"  Failed to save transcript: {e}")
                    stats["error"] += 1
                    stats["success"] -= 1
        else:
            error_type = result.get("error_type", "unknown")
            if error_type == "disabled":
                stats["disabled"] += 1
            elif error_type == "error":
                stats["error"] += 1
            else:
                stats["unavailable"] += 1
            
            if not args.quiet:
                log.info(f"  ✗ {result['reason']}")
        
        # Rate limiting
        if i < len(videos_to_process) - 1:
            time.sleep(args.delay)
        
        # Progress update every N videos
        if (i + 1) % PROGRESS_LOG_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed * 60  # videos per minute
            remaining = (stats["total"] - i - 1) / rate if rate > 0 else 0
            log.info(f"Progress: {i+1}/{stats['total']} "
                    f"({stats['success']} found, {stats['unavailable']} unavailable, "
                    f"{stats['disabled']} disabled, {stats['error']} errors) "
                    f"- {rate:.1f}/min, ~{remaining:.1f} min remaining")
    
    # Summary
    elapsed = time.time() - start_time
    
    log.info("")
    log.info("=" * 60)
    log.info("TRANSCRIPT FETCH SUMMARY")
    log.info("=" * 60)
    log.info(f"Total processed: {stats['total']}")
    log.info(f"  ✓ Found:       {stats['success']} ({100*stats['success']/stats['total']:.1f}%)")
    log.info(f"  ✗ Unavailable: {stats['unavailable']}")
    log.info(f"  ✗ Disabled:    {stats['disabled']}")
    log.info(f"  ✗ Errors:      {stats['error']}")
    log.info(f"Time elapsed: {elapsed/60:.1f} minutes")
    log.info(f"Rate: {stats['total']/elapsed*60:.1f} videos/minute")
    
    if args.dry_run:
        log.info("")
        log.info("DRY RUN - no transcripts were saved to database")


if __name__ == "__main__":
    main()
