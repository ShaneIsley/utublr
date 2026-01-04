"""
YouTube API fetcher module.
Handles all interactions with the YouTube Data API v3 and Transcript API.
"""

import os
import re
import sys
import time
import random  # Added for Jitter
import xml.etree.ElementTree as ET
from datetime import datetime
from functools import wraps
from typing import Optional, Callable, Any, List

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Try to import tqdm for progress bars, fallback to dummy if missing
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Logger setup
try:
    from logger import get_logger
    log = get_logger("youtube_api")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("youtube_api")

# youtube-transcript-api v1.x imports
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import (
        NoTranscriptFound,
        TranscriptsDisabled,
        VideoUnavailable,
    )
    TRANSCRIPT_API_AVAILABLE = True
except ImportError:
    TRANSCRIPT_API_AVAILABLE = False
    log.warning("youtube-transcript-api not installed, using fallback method")


# ============================================================================
# UTILITIES
# ============================================================================

def get_progress_bar(iterable, desc="", total=None, unit="it", enable=True):
    """Returns a tqdm progress bar if available, else returns the iterable."""
    if TQDM_AVAILABLE and enable:
        return tqdm(iterable, desc=desc, total=total, unit=unit, leave=False)
    return iterable

# ============================================================================
# RETRY LOGIC
# ============================================================================

RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_DELAY = 60.0

def retry_with_backoff(max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY, max_delay: float = MAX_DELAY, exponential_base: float = 2.0):
    """Decorator for retrying functions with exponential backoff."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    status_code = e.resp.status if hasattr(e, 'resp') else None
                    if status_code in RETRYABLE_STATUS_CODES:
                        last_exception = e
                        if attempt < max_retries:
                            delay = min(base_delay * (exponential_base ** attempt), max_delay)
                            retry_after = e.resp.get('retry-after') if hasattr(e, 'resp') else None
                            if retry_after:
                                try: delay = max(delay, float(retry_after))
                                except ValueError: pass
                            log.warning(f"HTTP {status_code}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})")
                            time.sleep(delay)
                            continue
                    else:
                        log.error(f"Non-retryable HTTP error {status_code}: {e}")
                        raise
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ConnectionResetError, TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        log.warning(f"Connection error, retrying in {delay:.1f}s: {e}")
                        time.sleep(delay)
                        continue
                except Exception as e:
                    log.error(f"Non-retryable error in {func.__name__}: {type(e).__name__}: {e}")
                    raise
            log.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
            raise last_exception
        return wrapper
    return decorator


# ============================================================================
# VALIDATORS
# ============================================================================

def validate_video_data(video: dict) -> dict:
    """Validate and sanitize video data."""
    if not video.get('video_id'): raise ValueError("Missing video_id")
    stats = video.get('statistics', {})
    for field in ['view_count', 'like_count', 'comment_count']:
        if field in stats:
            try: stats[field] = int(stats[field])
            except (ValueError, TypeError): stats[field] = 0
    
    # Duration sanitization
    if video.get('duration_seconds') is not None:
        try: video['duration_seconds'] = int(video['duration_seconds'])
        except (ValueError, TypeError): video['duration_seconds'] = None
    return video

def validate_channel_data(channel: dict) -> dict:
    """Validate and sanitize channel data."""
    if not channel.get('channel_id'): raise ValueError("Missing channel_id")
    stats = channel.get('statistics', {})
    for field in ['subscriber_count', 'view_count', 'video_count']:
        if field in stats:
            try: stats[field] = int(stats[field])
            except (ValueError, TypeError): stats[field] = 0
    return channel


# ============================================================================
# MAIN CLASS
# ============================================================================

class YouTubeFetcher:
    """Handles fetching data from YouTube API with rate limiting and retry logic."""
    
    def __init__(self, api_key: str = None, requests_per_second: float = 2.0):
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY not provided")
        
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.min_request_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        
        # Share this rate limit setting with the TranscriptFetcher
        TranscriptFetcher.set_rate_limit(requests_per_second)
        
        log.debug(f"YouTubeFetcher initialized, rate limit: {requests_per_second} req/s")
    
    def _rate_limit(self):
        """Enforce rate limiting with JITTER (randomness) to avoid fingerprinting."""
        elapsed = time.time() - self.last_request_time
        target_delay = self.min_request_interval
        
        if elapsed < target_delay:
            # Add 0-20% random jitter to the sleep time
            jitter = random.uniform(0, 0.2 * target_delay)
            sleep_time = (target_delay - elapsed) + jitter
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    @retry_with_backoff()
    def resolve_channel_id(self, identifier: str) -> str:
        """Resolve various channel identifiers to a channel ID."""
        identifier = identifier.strip()
        if identifier.startswith("UC") and len(identifier) == 24: return identifier
        
        if identifier.startswith("@"): handle = identifier.lstrip("@")
        elif "youtube.com/@" in identifier: 
            match = re.search(r"youtube\.com/@([\w.-]+)", identifier)
            handle = match.group(1) if match else None
        elif "youtube.com/channel/" in identifier:
            match = re.search(r"youtube\.com/channel/(UC[\w-]{22})", identifier)
            if match: return match.group(1)
            handle = None
        else: handle = identifier
        
        if handle:
            self._rate_limit()
            try:
                request = self.youtube.channels().list(part="id", forHandle=handle)
                response = request.execute()
                if response.get("items"): return response["items"][0]["id"]
            except Exception as e:
                log.warning(f"Failed to look up handle: {e}")
        
        raise ValueError(f"Could not resolve channel ID for: {identifier}")
    
    @retry_with_backoff()
    def fetch_channel(self, channel_id: str) -> dict:
        self._rate_limit()
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics,topicDetails,brandingSettings",
            id=channel_id
        )
        response = request.execute()
        if not response.get("items"): raise ValueError(f"Channel not found: {channel_id}")
        
        channel = response["items"][0]
        snippet = channel.get("snippet", {})
        stats = channel.get("statistics", {})
        branding = channel.get("brandingSettings", {})
        content = channel.get("contentDetails", {})
        thumbs = snippet.get("thumbnails", {})
        
        result = {
            "channel_id": channel_id,
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "country": snippet.get("country"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": (thumbs.get("high") or thumbs.get("default") or {}).get("url"),
            "banner_url": branding.get("image", {}).get("bannerExternalUrl"),
            "keywords": branding.get("channel", {}).get("keywords"),
            "topic_categories": channel.get("topicDetails", {}).get("topicCategories", []),
            "uploads_playlist_id": content.get("relatedPlaylists", {}).get("uploads"),
            "statistics": {
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "view_count": int(stats.get("viewCount", 0)),
                "video_count": int(stats.get("videoCount", 0)),
            }
        }
        return validate_channel_data(result)
    
    @retry_with_backoff()
    def _fetch_playlist_page(self, playlist_id: str, page_token: str = None) -> dict:
        self._rate_limit()
        return self.youtube.playlistItems().list(
            part="contentDetails", playlistId=playlist_id, maxResults=50, pageToken=page_token
        ).execute()
    
    def fetch_playlist_video_ids(self, playlist_id: str, max_results: int = None, use_progress_bar: bool = True) -> list[str]:
        """Fetch all video IDs from a playlist."""
        log.debug(f"Fetching video IDs from playlist: {playlist_id}")
        video_ids = []
        next_page = None
        
        # Initial estimation (can't know exactly without extra call, using generic progress)
        pbar = get_progress_bar(None, desc="Fetching Playlist Pages", unit="page", enable=use_progress_bar)
        
        try:
            while True:
                response = self._fetch_playlist_page(playlist_id, next_page)
                
                for item in response.get("items", []):
                    vid = item.get("contentDetails", {}).get("videoId")
                    if vid:
                        video_ids.append(vid)
                        if max_results and len(video_ids) >= max_results:
                            return video_ids
                
                if TQDM_AVAILABLE and use_progress_bar: pbar.update(1)
                
                next_page = response.get("nextPageToken")
                if not next_page: break
        finally:
             if TQDM_AVAILABLE and use_progress_bar: pbar.close()
        
        return video_ids
    
    @retry_with_backoff()
    def _fetch_videos_batch(self, video_ids: list[str]) -> list[dict]:
        self._rate_limit()
        return self.youtube.videos().list(
            part="snippet,contentDetails,statistics,status,topicDetails",
            id=",".join(video_ids)
        ).execute()
    
    def fetch_videos(self, video_ids: list[str], use_progress_bar: bool = True) -> list[dict]:
        """Fetch detailed metadata for videos with progress bar."""
        all_videos = []
        batches = [video_ids[i:i + 50] for i in range(0, len(video_ids), 50)]
        
        pbar = get_progress_bar(batches, desc="Fetching Video Metadata", unit="batch", enable=use_progress_bar)
        
        for batch in pbar:
            response = self._fetch_videos_batch(batch)
            for item in response.get("items", []):
                all_videos.append(validate_video_data(self._parse_video(item)))
                
        return all_videos
    
    def _parse_video(self, item: dict) -> dict:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        details = item.get("contentDetails", {})
        status = item.get("status", {})
        thumbs = snippet.get("thumbnails", {})
        
        duration_iso = details.get("duration", "")
        duration_sec = self._parse_duration(duration_iso)
        chapters = self._parse_chapters(snippet.get("description", ""), duration_sec)
        
        return {
            "video_id": item["id"],
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "duration_seconds": duration_sec,
            "duration_iso": duration_iso,
            "category_id": snippet.get("categoryId"),
            "default_language": snippet.get("defaultLanguage"),
            "default_audio_language": snippet.get("defaultAudioLanguage"),
            "tags": snippet.get("tags", []),
            "thumbnail_url": (thumbs.get("high") or thumbs.get("default") or {}).get("url"),
            "caption_available": details.get("caption") == "true",
            "privacy_status": status.get("privacyStatus"),
            "made_for_kids": status.get("madeForKids"),
            "topic_categories": item.get("topicDetails", {}).get("topicCategories", []),
            "has_chapters": len(chapters) > 0,
            "chapters": chapters,
            "statistics": {
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
            }
        }
    
    def _parse_duration(self, duration: str) -> Optional[int]:
        if not duration: return None
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match: return None
        h, m, s = int(match.group(1) or 0), int(match.group(2) or 0), int(match.group(3) or 0)
        return h * 3600 + m * 60 + s
    
    def _parse_chapters(self, description: str, duration_seconds: Optional[int] = None) -> list[dict]:
        if not description: return []
        chapters = []
        pattern = r'^[\s•\-\*]*(\d{1,2}:)?(\d{1,2}):(\d{2})[\s\-:]+(.+?)$'
        for line in description.split('\n'):
            match = re.match(pattern, line.strip())
            if match:
                h = int(match.group(1).rstrip(':')) if match.group(1) else 0
                m, s, title = int(match.group(2)), int(match.group(3)), match.group(4).strip()
                chapters.append({"title": title, "start_seconds": h * 3600 + m * 60 + s})
        
        for i, ch in enumerate(chapters):
            if i < len(chapters) - 1: ch["end_seconds"] = chapters[i + 1]["start_seconds"]
            elif duration_seconds: ch["end_seconds"] = duration_seconds
        return chapters
    
    @retry_with_backoff()
    def _fetch_comments_page(self, video_id: str, max_results: int, page_token: str = None) -> dict:
        self._rate_limit()
        return self.youtube.commentThreads().list(
            part="snippet,replies", videoId=video_id, maxResults=min(100, max_results),
            pageToken=page_token
        ).execute()
    
    def fetch_comments(self, video_id: str, since: datetime = None, max_results: int = 500, max_replies_per_comment: int = 10, use_progress_bar: bool = False) -> list[dict]:
        """Fetch comments for a video with optional progress bar."""
        comments = []
        next_page = None
        
        # We don't know total comments upfront efficiently, so we use a counter bar
        pbar = get_progress_bar(None, desc=f"Comments {video_id}", unit="cmts", enable=use_progress_bar)
        
        try:
            while len(comments) < max_results:
                response = self._fetch_comments_page(video_id, max_results - len(comments), next_page)
                
                fetched_in_batch = 0
                found_old = False
                
                for item in response.get("items", []):
                    top = item["snippet"]["topLevelComment"]["snippet"]
                    pub_at = top.get("publishedAt")
                    
                    if since and pub_at:
                        c_time = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                        if since.tzinfo is None: since = since.replace(tzinfo=c_time.tzinfo)
                        if c_time <= since:
                            found_old = True
                            break
                    
                    comments.append({
                        "comment_id": item["id"], "video_id": video_id, "parent_comment_id": None,
                        "text": top.get("textDisplay"), "like_count": top.get("likeCount", 0), "published_at": pub_at
                    })
                    fetched_in_batch += 1
                    
                    if "replies" in item:
                        replies = item["replies"]["comments"][:max_replies_per_comment]
                        for r in replies:
                            rs = r["snippet"]
                            comments.append({
                                "comment_id": r["id"], "video_id": video_id, "parent_comment_id": item["id"],
                                "text": rs.get("textDisplay"), "like_count": rs.get("likeCount", 0), "published_at": rs.get("publishedAt")
                            })
                            fetched_in_batch += 1
                
                if TQDM_AVAILABLE and use_progress_bar: pbar.update(fetched_in_batch)
                
                if found_old: break
                next_page = response.get("nextPageToken")
                if not next_page: break
                
        except HttpError as e:
            if e.resp.status == 403: return [] # Comments disabled
            raise
        finally:
            if TQDM_AVAILABLE and use_progress_bar: pbar.close()
        
        return comments
    
    @retry_with_backoff()
    def _fetch_playlists_page(self, channel_id: str, page_token: str = None) -> dict:
        self._rate_limit()
        return self.youtube.playlists().list(
            part="snippet,contentDetails,status", channelId=channel_id, maxResults=50, pageToken=page_token
        ).execute()
    
    def fetch_playlists(self, channel_id: str) -> list[dict]:
        playlists = []
        next_page = None
        while True:
            response = self._fetch_playlists_page(channel_id, next_page)
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                thumbs = snippet.get("thumbnails", {})
                playlists.append({
                    "playlist_id": item["id"],
                    "channel_id": channel_id,
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail_url": (thumbs.get("high") or thumbs.get("default") or {}).get("url"),
                    "item_count": item.get("contentDetails", {}).get("itemCount", 0),
                })
            next_page = response.get("nextPageToken")
            if not next_page: break
        return playlists


class TranscriptFetcher:
    """Handles fetching video transcripts with Rate Limiting."""
    
    # Class-level rate limiting to share across instances/calls
    min_request_interval = 0.5 # Default
    last_request_time = 0
    
    @classmethod
    def set_rate_limit(cls, requests_per_second: float):
        cls.min_request_interval = 1.0 / requests_per_second

    @classmethod
    def _rate_limit(cls):
        """Enforce rate limiting with Jitter."""
        elapsed = time.time() - cls.last_request_time
        if elapsed < cls.min_request_interval:
            # Jitter: 0 to 20% extra
            jitter = random.uniform(0, 0.2 * cls.min_request_interval)
            time.sleep((cls.min_request_interval - elapsed) + jitter)
        cls.last_request_time = time.time()

    @staticmethod
    def fetch(video_id: str, language: str = "en") -> Optional[dict]:
        """Fetch transcript using v1.x API objects."""
        # Enforce rate limit before ANY fetch attempt
        TranscriptFetcher._rate_limit()
        
        if TRANSCRIPT_API_AVAILABLE:
            result = TranscriptFetcher._fetch_with_api(video_id, language)
            if result and result.get("available"):
                return result
        
        return TranscriptFetcher._fetch_fallback(video_id)
    
    @staticmethod
    def _fetch_with_api(video_id: str, language: str = "en") -> Optional[dict]:
        try:
            api = YouTubeTranscriptApi() # Fresh instance per request
            transcript_list = api.list(video_id)
            
            transcript = None
            transcript_info = {}
            
            # 1. Manual
            try:
                for t in transcript_list:
                    if not t.is_generated and t.language_code in [language, 'en', 'en-US', 'en-GB']:
                        transcript = t
                        transcript_info = {"transcript_type": "manual", "language": t.language, "language_code": t.language_code}
                        break
            except Exception: pass
            
            # 2. Auto
            if not transcript:
                try:
                    transcript = transcript_list.find_transcript([language, 'en', 'en-US', 'en-GB'])
                    transcript_info = {"transcript_type": "auto", "language": transcript.language, "language_code": transcript.language_code}
                except NoTranscriptFound: pass
            
            # 3. Translate
            if not transcript:
                try:
                    for t in transcript_list:
                        if t.is_translatable:
                            en_avail = False
                            for lang in t.translation_languages:
                                code = lang.get('language_code') if isinstance(lang, dict) else getattr(lang, 'language_code', '')
                                if code.startswith('en'): en_avail = True; break
                            if en_avail:
                                transcript = t.translate('en')
                                transcript_info = {"transcript_type": "translated", "language": "en", "original": t.language}
                                break
                except Exception: pass
            
            if transcript:
                data = transcript.fetch()
                entries = [{"start": s.start, "duration": s.duration, "end": s.start+s.duration, "text": s.text} for s in data]
                return {"available": True, **transcript_info, "entries": entries, "full_text": " ".join(e["text"] for e in entries)}
            
            return {"available": False, "reason": "No English transcript found"}
            
        except TranscriptsDisabled: return {"available": False, "reason": "Transcripts disabled"}
        except VideoUnavailable: return {"available": False, "reason": "Video unavailable"}
        except Exception as e: return {"available": False, "reason": str(e)}
    
    @staticmethod
    def _fetch_fallback(video_id: str) -> Optional[dict]:
        try:
            url = f"https://www.youtube.com/watch?v={video_id}"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if resp.status_code != 200: return {"available": False, "reason": "Page fetch failed"}
            
            match = re.search(r'"captions":.*?"captionTracks":\[(.*?)\]', resp.text)
            if not match: return {"available": False, "reason": "No captions found"}
            
            tracks = match.group(1)
            en_match = re.search(r'"baseUrl":"([^"]+)"[^}]*"languageCode":"(en[^"]*)"', tracks)
            cap_url = (en_match.group(1) if en_match else re.search(r'"baseUrl":"([^"]+)"', tracks).group(1)).replace("\\u0026", "&")
            
            cap_resp = requests.get(cap_url, timeout=10)
            root = ET.fromstring(cap_resp.text)
            
            entries = []
            for t in root.findall(".//text"):
                start = float(t.get("start", 0))
                dur = float(t.get("dur", 0))
                text = (t.text or "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&quot;", '"')
                entries.append({"start": start, "duration": dur, "end": start+dur, "text": text})
                
            return {"available": True, "transcript_type": "fallback", "language": "en", "entries": entries, "full_text": " ".join(e["text"] for e in entries)}
        except Exception as e: return {"available": False, "reason": str(e)}
