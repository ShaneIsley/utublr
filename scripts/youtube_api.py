"""
YouTube API fetcher module.
Handles all interactions with the YouTube Data API v3.

Features:
- Retry with exponential backoff for transient errors
- Rate limiting between requests
- Data validation
"""

import os
import re
import ssl
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from functools import wraps
from typing import Optional, Callable, Any

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from logger import get_logger

log = get_logger("youtube_api")


# ============================================================================
# RETRY LOGIC WITH EXPONENTIAL BACKOFF
# ============================================================================

RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_DELAY = 60.0


def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    max_delay: float = MAX_DELAY,
    exponential_base: float = 2.0,
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Retries on:
    - HTTP 429 (rate limit)
    - HTTP 5xx (server errors)
    - Connection errors
    """
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
                            
                            # Check for Retry-After header
                            retry_after = e.resp.get('retry-after') if hasattr(e, 'resp') else None
                            if retry_after:
                                try:
                                    delay = max(delay, float(retry_after))
                                except ValueError:
                                    pass
                            
                            log.warning(f"HTTP {status_code} error, retrying in {delay:.1f}s "
                                       f"(attempt {attempt + 1}/{max_retries + 1}): {e}")
                            time.sleep(delay)
                            continue
                    else:
                        # Non-retryable HTTP error
                        log.error(f"Non-retryable HTTP error {status_code}: {e}")
                        raise
                        
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        ConnectionResetError,
                        TimeoutError,
                        ssl.SSLError,
                        OSError) as e:
                    # OSError catches low-level network errors including SSLEOFError
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        log.warning(f"Connection error, retrying in {delay:.1f}s "
                                   f"(attempt {attempt + 1}/{max_retries + 1}): {type(e).__name__}: {e}")
                        time.sleep(delay)
                        continue
                    
                except Exception as e:
                    # Non-retryable exception
                    log.error(f"Non-retryable error in {func.__name__}: {type(e).__name__}: {e}")
                    raise
            
            # All retries exhausted
            log.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
            raise last_exception
        
        return wrapper
    return decorator


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_video_data(video: dict) -> dict:
    """Validate and sanitize video data from API response."""
    required_fields = ['video_id']
    
    for field in required_fields:
        if not video.get(field):
            raise ValueError(f"Missing required field: {field}")
    
    # Ensure numeric fields are integers
    stats = video.get('statistics', {})
    for field in ['view_count', 'like_count', 'comment_count']:
        if field in stats:
            try:
                stats[field] = int(stats[field])
            except (ValueError, TypeError):
                stats[field] = 0
    
    # Ensure duration is valid
    duration = video.get('duration_seconds')
    if duration is not None:
        try:
            video['duration_seconds'] = int(duration)
        except (ValueError, TypeError):
            video['duration_seconds'] = None
    
    return video


def validate_channel_data(channel: dict) -> dict:
    """Validate and sanitize channel data from API response."""
    required_fields = ['channel_id']
    
    for field in required_fields:
        if not channel.get(field):
            raise ValueError(f"Missing required field: {field}")
    
    # Ensure numeric fields are integers
    stats = channel.get('statistics', {})
    for field in ['subscriber_count', 'view_count', 'video_count']:
        if field in stats:
            try:
                stats[field] = int(stats[field])
            except (ValueError, TypeError):
                stats[field] = 0
    
    return channel


class YouTubeFetcher:
    """Handles fetching data from YouTube API with rate limiting and retry logic."""
    
    def __init__(self, api_key: str = None, requests_per_second: float = 2.0):
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY not provided")
        
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.min_request_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        
        log.debug(f"YouTubeFetcher initialized, rate limit: {requests_per_second} req/s")
    
    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            log.debug(f"Rate limiting: sleeping {sleep_time:.3f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    @retry_with_backoff()
    def resolve_channel_id(self, identifier: str) -> str:
        """Resolve various channel identifiers to a channel ID."""
        log.debug(f"Resolving channel identifier: {identifier}")
        identifier = identifier.strip()
        
        # Direct channel ID
        if identifier.startswith("UC") and len(identifier) == 24:
            log.debug(f"Direct channel ID: {identifier}")
            return identifier
        
        # Handle (@username)
        if identifier.startswith("@"):
            handle = identifier.lstrip("@")
        elif "youtube.com/@" in identifier:
            match = re.search(r"youtube\.com/@([\w.-]+)", identifier)
            handle = match.group(1) if match else None
        elif "youtube.com/channel/" in identifier:
            match = re.search(r"youtube\.com/channel/(UC[\w-]{22})", identifier)
            if match:
                log.debug(f"Extracted channel ID from URL: {match.group(1)}")
                return match.group(1)
            handle = None
        else:
            # Assume it's a handle without @
            handle = identifier
        
        if handle:
            log.debug(f"Looking up handle: {handle}")
            self._rate_limit()
            request = self.youtube.channels().list(
                part="id",
                forHandle=handle
            )
            response = request.execute()
            
            if response.get("items"):
                channel_id = response["items"][0]["id"]
                log.debug(f"Resolved handle '{handle}' to channel ID: {channel_id}")
                return channel_id
        
        raise ValueError(f"Could not resolve channel ID for: {identifier}")
    
    @retry_with_backoff()
    def fetch_channel(self, channel_id: str) -> dict:
        """Fetch comprehensive channel metadata."""
        log.debug(f"Fetching channel metadata for: {channel_id}")
        self._rate_limit()
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics,topicDetails,brandingSettings",
            id=channel_id
        )
        response = request.execute()
        
        if not response.get("items"):
            raise ValueError(f"Channel not found: {channel_id}")
        
        channel = response["items"][0]
        snippet = channel.get("snippet", {})
        statistics = channel.get("statistics", {})
        branding = channel.get("brandingSettings", {})
        content_details = channel.get("contentDetails", {})
        
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (thumbnails.get("high") or thumbnails.get("default") or {}).get("url")
        
        result = {
            "channel_id": channel_id,
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "custom_url": snippet.get("customUrl"),
            "country": snippet.get("country"),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": thumbnail_url,
            "banner_url": branding.get("image", {}).get("bannerExternalUrl"),
            "keywords": branding.get("channel", {}).get("keywords"),
            "topic_categories": channel.get("topicDetails", {}).get("topicCategories", []),
            "uploads_playlist_id": content_details.get("relatedPlaylists", {}).get("uploads"),
            "statistics": {
                "subscriber_count": int(statistics.get("subscriberCount", 0)),
                "view_count": int(statistics.get("viewCount", 0)),
                "video_count": int(statistics.get("videoCount", 0)),
            }
        }
        
        log.debug(f"Channel '{result['title']}': {result['statistics']['video_count']} videos")
        return validate_channel_data(result)
    
    @retry_with_backoff()
    def _fetch_playlist_page(self, playlist_id: str, page_token: str = None) -> dict:
        """Fetch a single page of playlist items (internal, with retry)."""
        self._rate_limit()
        request = self.youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token
        )
        return request.execute()
    
    def fetch_playlist_video_ids(self, playlist_id: str, max_results: int = None) -> list[str]:
        """Fetch all video IDs from a playlist."""
        log.debug(f"Fetching video IDs from playlist: {playlist_id}")
        video_ids = []
        next_page_token = None
        page_count = 0
        
        while True:
            response = self._fetch_playlist_page(playlist_id, next_page_token)
            page_count += 1
            
            for item in response.get("items", []):
                video_id = item.get("contentDetails", {}).get("videoId")
                if video_id:
                    video_ids.append(video_id)
                    if max_results and len(video_ids) >= max_results:
                        log.debug(f"Reached max_results ({max_results}), stopping")
                        return video_ids
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
            
            if page_count % 10 == 0:
                log.debug(f"Playlist fetch progress: {len(video_ids)} videos, page {page_count}")
        
        log.debug(f"Fetched {len(video_ids)} video IDs from playlist in {page_count} pages")
        return video_ids
    
    @retry_with_backoff()
    def _search_channel_videos_page(
        self, 
        channel_id: str, 
        published_after: datetime = None,
        page_token: str = None
    ) -> dict:
        """Search for videos from a channel, ordered by date (internal, with retry)."""
        self._rate_limit()
        
        kwargs = {
            "part": "id",
            "channelId": channel_id,
            "type": "video",
            "order": "date",
            "maxResults": 50,
        }
        
        if published_after:
            # Format as RFC 3339
            if published_after.tzinfo is None:
                published_after = published_after.replace(tzinfo=timezone.utc)
            kwargs["publishedAfter"] = published_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if page_token:
            kwargs["pageToken"] = page_token
            
        request = self.youtube.search().list(**kwargs)
        return request.execute()
    
    def search_channel_videos(
        self,
        channel_id: str,
        published_after: datetime = None,
        known_video_ids: set = None,
        max_results: int = None,
    ) -> tuple[list[str], int]:
        """
        Search for videos from a channel, ordered by date (newest first).
        
        This uses the search API (100 units/call) but allows early stopping
        when we encounter videos we already have.
        
        Args:
            channel_id: YouTube channel ID
            published_after: Only return videos published after this time
            known_video_ids: Set of video IDs we already have - stop when we hit one
            max_results: Maximum videos to return
            
        Returns:
            Tuple of (list of video IDs, number of API calls made)
        """
        log.debug(f"Searching for new videos from channel {channel_id}")
        video_ids = []
        next_page_token = None
        api_calls = 0
        
        while True:
            response = self._search_channel_videos_page(
                channel_id, 
                published_after=published_after,
                page_token=next_page_token
            )
            api_calls += 1
            
            found_known = False
            for item in response.get("items", []):
                video_id = item.get("id", {}).get("videoId")
                if not video_id:
                    continue
                    
                # Stop if we hit a video we already have
                if known_video_ids and video_id in known_video_ids:
                    log.debug(f"Found known video {video_id}, stopping search")
                    found_known = True
                    break
                    
                video_ids.append(video_id)
                
                if max_results and len(video_ids) >= max_results:
                    log.debug(f"Reached max_results ({max_results}), stopping")
                    return video_ids, api_calls
            
            if found_known:
                break
                
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
                
            # Safety limit - don't paginate forever
            if api_calls >= 10:
                log.warning(f"Search pagination limit reached ({api_calls} calls), stopping")
                break
        
        log.debug(f"Search found {len(video_ids)} new videos in {api_calls} API calls")
        return video_ids, api_calls
    
    @retry_with_backoff()
    def _fetch_videos_batch(self, video_ids: list[str]) -> list[dict]:
        """Fetch a batch of videos (internal, with retry)."""
        self._rate_limit()
        request = self.youtube.videos().list(
            part="snippet,contentDetails,statistics,status,topicDetails",
            id=",".join(video_ids)
        )
        return request.execute()
    
    def fetch_videos(self, video_ids: list[str]) -> list[dict]:
        """Fetch detailed metadata for videos (handles batching)."""
        log.debug(f"Fetching metadata for {len(video_ids)} videos")
        all_videos = []
        
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            batch_num = i // 50 + 1
            total_batches = (len(video_ids) + 49) // 50
            
            log.debug(f"Fetching video batch {batch_num}/{total_batches} ({len(batch)} videos)")
            
            response = self._fetch_videos_batch(batch)
            
            for item in response.get("items", []):
                video = self._parse_video(item)
                all_videos.append(validate_video_data(video))
        
        log.debug(f"Fetched metadata for {len(all_videos)} videos")
        return all_videos

    def _parse_video(self, item: dict) -> dict:
        """Parse video API response into our schema."""
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        status = item.get("status", {})
        
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = (thumbnails.get("high") or thumbnails.get("default") or {}).get("url")
        
        duration_iso = content_details.get("duration", "")
        duration_seconds = self._parse_duration(duration_iso)
        
        # Parse chapters from description
        chapters = self._parse_chapters(snippet.get("description", ""), duration_seconds)
        
        return {
            "video_id": item["id"],
            "channel_id": snippet.get("channelId"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "published_at": snippet.get("publishedAt"),
            "duration_seconds": duration_seconds,
            "duration_iso": duration_iso,
            "category_id": snippet.get("categoryId"),
            "default_language": snippet.get("defaultLanguage"),
            "default_audio_language": snippet.get("defaultAudioLanguage"),
            "tags": snippet.get("tags", []),
            "thumbnail_url": thumbnail_url,
            "caption_available": content_details.get("caption") == "true",
            "definition": content_details.get("definition"),
            "dimension": content_details.get("dimension"),
            "projection": content_details.get("projection"),
            "privacy_status": status.get("privacyStatus"),
            "license": status.get("license"),
            "embeddable": status.get("embeddable"),
            "made_for_kids": status.get("madeForKids"),
            "topic_categories": item.get("topicDetails", {}).get("topicCategories", []),
            "has_chapters": len(chapters) > 0,
            "chapters": chapters,
            "statistics": {
                "view_count": int(statistics.get("viewCount", 0)),
                "like_count": int(statistics.get("likeCount", 0)),
                "comment_count": int(statistics.get("commentCount", 0)),
            }
        }
    
    def _parse_duration(self, duration: str) -> Optional[int]:
        """Convert ISO 8601 duration (PT1H2M3S) to seconds."""
        if not duration:
            return None
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match:
            return None
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds
    
    def _parse_chapters(self, description: str, duration_seconds: Optional[int] = None) -> list[dict]:
        """Extract chapter timestamps from video description."""
        if not description:
            return []
        
        chapters = []
        timestamp_pattern = r'^[\s•\-\*]*(\d{1,2}:)?(\d{1,2}):(\d{2})[\s\-:]+(.+?)$'
        
        for line in description.split('\n'):
            match = re.match(timestamp_pattern, line.strip())
            if match:
                hours = int(match.group(1).rstrip(':')) if match.group(1) else 0
                minutes = int(match.group(2))
                seconds = int(match.group(3))
                title = match.group(4).strip()
                
                start_seconds = hours * 3600 + minutes * 60 + seconds
                chapters.append({
                    "title": title,
                    "start_seconds": start_seconds,
                })
        
        # Add end times
        for i, chapter in enumerate(chapters):
            if i < len(chapters) - 1:
                chapter["end_seconds"] = chapters[i + 1]["start_seconds"]
            elif duration_seconds:
                chapter["end_seconds"] = duration_seconds
        
        if chapters:
            log.debug(f"Parsed {len(chapters)} chapters from description")
        
        return chapters
    
    @retry_with_backoff()
    def _fetch_comments_page(self, video_id: str, max_results: int, page_token: str = None) -> dict:
        """Fetch a single page of comments (internal, with retry)."""
        self._rate_limit()
        request = self.youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=min(100, max_results),
            pageToken=page_token,
            order="time",
            textFormat="plainText"
        )
        return request.execute()
    
    def fetch_comments(
        self, 
        video_id: str, 
        since: datetime = None, 
        max_results: int = 500,
        max_replies_per_comment: int = 10
    ) -> list[dict]:
        """
        Fetch comments for a video.
        
        Args:
            video_id: YouTube video ID
            since: Only fetch comments newer than this time
            max_results: Maximum total comments to fetch
            max_replies_per_comment: Limit replies per top-level comment (prevents blowup)
        """
        log.debug(f"Fetching comments for video {video_id}, max={max_results}, since={since}")
        comments = []
        next_page_token = None
        pages_fetched = 0
        
        try:
            while len(comments) < max_results:
                response = self._fetch_comments_page(video_id, max_results - len(comments), next_page_token)
                pages_fetched += 1
                
                found_old = False
                for item in response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]
                    published_at = top_comment.get("publishedAt")
                    
                    # Check if we've reached comments older than our cutoff
                    if since and published_at:
                        comment_time = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        if since.tzinfo is None:
                            since = since.replace(tzinfo=comment_time.tzinfo)
                        if comment_time <= since:
                            found_old = True
                            log.debug(f"Reached comment older than cutoff, stopping")
                            break
                    
                    comments.append({
                        "comment_id": item["id"],
                        "video_id": video_id,
                        "parent_comment_id": None,
                        "author_display_name": top_comment.get("authorDisplayName"),
                        "author_channel_id": top_comment.get("authorChannelId", {}).get("value"),
                        "text": top_comment.get("textDisplay"),
                        "like_count": top_comment.get("likeCount", 0),
                        "published_at": published_at,
                        "updated_at": top_comment.get("updatedAt"),
                    })
                    
                    # Get replies (with limit to prevent blowup on viral comments)
                    if "replies" in item:
                        replies = item["replies"]["comments"][:max_replies_per_comment]
                        for reply in replies:
                            reply_snippet = reply["snippet"]
                            comments.append({
                                "comment_id": reply["id"],
                                "video_id": video_id,
                                "parent_comment_id": item["id"],
                                "author_display_name": reply_snippet.get("authorDisplayName"),
                                "author_channel_id": reply_snippet.get("authorChannelId", {}).get("value"),
                                "text": reply_snippet.get("textDisplay"),
                                "like_count": reply_snippet.get("likeCount", 0),
                                "published_at": reply_snippet.get("publishedAt"),
                                "updated_at": reply_snippet.get("updatedAt"),
                            })
                        
                        if len(item["replies"]["comments"]) > max_replies_per_comment:
                            log.debug(f"Truncated replies: {len(item['replies']['comments'])} -> {max_replies_per_comment}")
                
                if found_old:
                    break
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
        except HttpError as e:
            if e.resp.status == 403:
                # Comments disabled
                log.debug(f"Comments disabled for video {video_id}")
                return []
            raise
        
        log.debug(f"Fetched {len(comments)} comments for video {video_id} in {pages_fetched} pages")
        return comments
    
    @retry_with_backoff()
    def _fetch_playlists_page(self, channel_id: str, page_token: str = None) -> dict:
        """Fetch a single page of playlists (internal, with retry)."""
        self._rate_limit()
        request = self.youtube.playlists().list(
            part="snippet,contentDetails,status",
            channelId=channel_id,
            maxResults=50,
            pageToken=page_token
        )
        return request.execute()
    
    def fetch_playlists(self, channel_id: str) -> list[dict]:
        """Fetch all playlists for a channel."""
        log.debug(f"Fetching playlists for channel {channel_id}")
        playlists = []
        next_page_token = None
        
        while True:
            response = self._fetch_playlists_page(channel_id, next_page_token)
            
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                thumbnails = snippet.get("thumbnails", {})
                thumbnail_url = (thumbnails.get("high") or thumbnails.get("default") or {}).get("url")
                
                playlists.append({
                    "playlist_id": item["id"],
                    "channel_id": channel_id,
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail_url": thumbnail_url,
                    "item_count": item.get("contentDetails", {}).get("itemCount", 0),
                    "privacy_status": item.get("status", {}).get("privacyStatus"),
                })
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        
        log.debug(f"Fetched {len(playlists)} playlists for channel {channel_id}")
        return playlists
