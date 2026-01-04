#!/usr/bin/env python3
"""
Test script to debug transcript fetching.

Usage:
    python test_transcript.py VIDEO_ID [VIDEO_ID2 ...]
    python test_transcript.py --channel CHANNEL_ID [--limit N]
    python test_transcript.py  # Uses default test videos
"""

import sys
import argparse

# Test with youtube-transcript-api directly
print("=" * 60)
print("Testing youtube-transcript-api v1.x")
print("=" * 60)

try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import (
        NoTranscriptFound,
        TranscriptsDisabled,
        VideoUnavailable,
        NoTranscriptAvailable,
    )
    print("✓ youtube-transcript-api imported successfully")
except ImportError as e:
    print(f"✗ Failed to import youtube-transcript-api: {e}")
    sys.exit(1)

# For channel lookup
try:
    from googleapiclient.discovery import build
    import os
    YOUTUBE_API_AVAILABLE = bool(os.environ.get("YOUTUBE_API_KEY"))
except ImportError:
    YOUTUBE_API_AVAILABLE = False

# Test videos - mix of known good/bad cases
TEST_VIDEOS = [
    ("dQw4w9WgXcQ", "Rick Astley - Never Gonna Give You Up"),
    ("jNQXAC9IVRw", "Me at the zoo (first YouTube video)"),
]


def test_video(video_id: str, description: str = "", verbose: bool = True) -> dict:
    """
    Test transcript fetching for a single video.
    
    Returns dict with:
        - success: bool
        - reason: str (why it failed or succeeded)
        - transcript_type: str (if successful)
        - available_transcripts: list of available transcript info
    """
    result = {
        "video_id": video_id,
        "success": False,
        "reason": "Unknown",
        "transcript_type": None,
        "available_transcripts": [],
        "error_type": None,
    }
    
    if verbose:
        print(f"\n--- Testing: {video_id} ({description}) ---")
    
    try:
        # Create API instance (new v1.x API)
        api = YouTubeTranscriptApi()
        
        # List available transcripts
        transcript_list = api.list(video_id)
        
        # Record what's available
        for t in transcript_list:
            result["available_transcripts"].append({
                "language": t.language,
                "language_code": t.language_code,
                "is_generated": t.is_generated,
                "is_translatable": bool(t.translation_languages),
            })
        
        if verbose:
            print(f"Available transcripts: {len(result['available_transcripts'])}")
            for t in result["available_transcripts"]:
                print(f"  - {t['language']} ({t['language_code']}): "
                      f"{'auto-generated' if t['is_generated'] else 'manual'}, "
                      f"translatable: {t['is_translatable']}")
        
        # Try to find manual English transcript first
        manual_transcript = None
        for t in transcript_list:
            if not t.is_generated and t.language_code in ['en', 'en-US', 'en-GB']:
                manual_transcript = t
                break
        
        if manual_transcript:
            if verbose:
                print(f"✓ Found MANUAL English transcript ({manual_transcript.language_code})")
            data = manual_transcript.fetch()
            result["success"] = True
            result["reason"] = f"Manual transcript ({manual_transcript.language_code})"
            result["transcript_type"] = "manual"
            if verbose:
                print(f"  Entries: {len(data)}")
                if data:
                    print(f"  First entry: {data[0].text[:50]}...")
            return result
        
        # Fall back to any English transcript (including auto-generated)
        try:
            transcript = transcript_list.find_transcript(['en', 'en-US', 'en-GB'])
            if verbose:
                print(f"✓ Found {'auto-generated' if transcript.is_generated else 'manual'} English transcript")
            data = transcript.fetch()
            result["success"] = True
            result["reason"] = f"{'Auto-generated' if transcript.is_generated else 'Manual'} ({transcript.language_code})"
            result["transcript_type"] = "auto-generated" if transcript.is_generated else "manual"
            if verbose:
                print(f"  Entries: {len(data)}")
                if data:
                    print(f"  First entry: {data[0].text[:50]}...")
            return result
        except NoTranscriptFound:
            if verbose:
                print("  No direct English transcript")
        
        # Try translation
        for t in transcript_list:
            if t.translation_languages:
                en_available = any(
                    lang.get('language_code', '').startswith('en') 
                    for lang in t.translation_languages
                )
                if en_available:
                    translated = t.translate('en')
                    if verbose:
                        print(f"✓ Found translatable transcript from {t.language}")
                    data = translated.fetch()
                    result["success"] = True
                    result["reason"] = f"Translated from {t.language}"
                    result["transcript_type"] = "translated"
                    if verbose:
                        print(f"  Entries: {len(data)}")
                    return result
        
        result["reason"] = "No English transcript and no translatable transcripts"
        result["error_type"] = "no_english"
        if verbose:
            print(f"✗ {result['reason']}")
        return result
        
    except TranscriptsDisabled:
        result["reason"] = "Transcripts disabled by uploader"
        result["error_type"] = "disabled"
        if verbose:
            print(f"✗ {result['reason']}")
        return result
    except VideoUnavailable:
        result["reason"] = "Video unavailable (private/deleted/region-blocked)"
        result["error_type"] = "unavailable"
        if verbose:
            print(f"✗ {result['reason']}")
        return result
    except NoTranscriptAvailable:
        result["reason"] = "No transcripts available for this video"
        result["error_type"] = "no_transcripts"
        if verbose:
            print(f"✗ {result['reason']}")
        return result
    except Exception as e:
        result["reason"] = f"{type(e).__name__}: {e}"
        result["error_type"] = "error"
        if verbose:
            print(f"✗ Error: {result['reason']}")
            import traceback
            traceback.print_exc()
        return result


def get_channel_videos(channel_id: str, limit: int = 20) -> list:
    """Fetch video IDs from a channel using YouTube API."""
    if not YOUTUBE_API_AVAILABLE:
        print("✗ YouTube API not available (set YOUTUBE_API_KEY)")
        return []
    
    api_key = os.environ.get("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key)
    
    # Get uploads playlist
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()
    
    if not channel_response.get("items"):
        # Try by handle/username
        channel_response = youtube.channels().list(
            part="contentDetails",
            forHandle=channel_id.lstrip("@")
        ).execute()
    
    if not channel_response.get("items"):
        print(f"✗ Channel not found: {channel_id}")
        return []
    
    uploads_playlist = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    # Get videos from playlist
    videos = []
    next_page = None
    
    while len(videos) < limit:
        playlist_response = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist,
            maxResults=min(50, limit - len(videos)),
            pageToken=next_page
        ).execute()
        
        for item in playlist_response.get("items", []):
            videos.append({
                "video_id": item["snippet"]["resourceId"]["videoId"],
                "title": item["snippet"]["title"]
            })
        
        next_page = playlist_response.get("nextPageToken")
        if not next_page:
            break
    
    return videos[:limit]


def main():
    parser = argparse.ArgumentParser(description="Test transcript fetching")
    parser.add_argument("video_ids", nargs="*", help="Video IDs to test")
    parser.add_argument("--channel", "-c", help="Channel ID or @handle to test")
    parser.add_argument("--limit", "-l", type=int, default=20, help="Number of videos to test from channel")
    parser.add_argument("--quiet", "-q", action="store_true", help="Only show summary")
    
    args = parser.parse_args()
    
    videos_to_test = []
    
    if args.channel:
        print(f"\nFetching videos from channel: {args.channel}")
        channel_videos = get_channel_videos(args.channel, args.limit)
        if channel_videos:
            videos_to_test = [(v["video_id"], v["title"][:40]) for v in channel_videos]
            print(f"Found {len(videos_to_test)} videos to test\n")
    elif args.video_ids:
        videos_to_test = [(vid, "command line") for vid in args.video_ids]
    else:
        videos_to_test = TEST_VIDEOS
    
    if not videos_to_test:
        print("No videos to test!")
        return
    
    # Run tests
    results = []
    for video_id, description in videos_to_test:
        result = test_video(video_id, description, verbose=not args.quiet)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    
    print(f"\nTotal: {len(successes)}/{len(results)} successful ({100*len(successes)/len(results):.1f}%)")
    
    # Group failures by reason
    if failures:
        print(f"\nFailure breakdown:")
        failure_reasons = {}
        for r in failures:
            error_type = r.get("error_type", "unknown")
            if error_type not in failure_reasons:
                failure_reasons[error_type] = []
            failure_reasons[error_type].append(r)
        
        for error_type, items in sorted(failure_reasons.items(), key=lambda x: -len(x[1])):
            print(f"  {error_type}: {len(items)} videos")
            if len(items) <= 5:
                for r in items:
                    print(f"    - {r['video_id']}: {r['reason']}")
            else:
                for r in items[:3]:
                    print(f"    - {r['video_id']}: {r['reason']}")
                print(f"    ... and {len(items) - 3} more")
    
    # Show transcript types for successes
    if successes:
        print(f"\nSuccess breakdown:")
        success_types = {}
        for r in successes:
            t = r.get("transcript_type", "unknown")
            success_types[t] = success_types.get(t, 0) + 1
        for t, count in sorted(success_types.items(), key=lambda x: -x[1]):
            print(f"  {t}: {count} videos")


if __name__ == "__main__":
    main()
