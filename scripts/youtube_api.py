"""
YouTube API fetcher module.
Handles all interactions with the YouTube Data API v3.
"""

import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Optional: youtube-transcript-api for easier transcript fetching
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


class YouTubeFetcher:
    """Handles fetching data from YouTube API with rate limiting."""
    
    def __init__(self, api_key: str = None, requests_per_second: float = 2.0):
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY not provided")
        
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.min_request_interval = 1.0 / requests_per_second
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
    
    def resolve_channel_id(self, identifier: str) -> str:
        """Resolve various channel identifiers to a channel ID."""
        identifier = identifier.strip()
        
        # Direct channel ID
        if identifier.startswith("UC") and len(identifier) == 24:
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
                return match.group(1)
            handle = None
        else:
            # Assume it's a handle without @
            handle = identifier
        
        if handle:
            self._rate_limit()
            request = self.youtube.channels().list(
                part="id",
                forHandle=handle
            )
            response = request.execute()
            
            if response.get("items"):
                return response["items"][0]["id"]
        
        raise ValueError(f"Could not resolve channel ID for: {identifier}")
    
    def fetch_channel(self, channel_id: str) -> dict:
        """Fetch comprehensive channel metadata."""
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
        
        return {
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
    
    def fetch_playlist_video_ids(self, playlist_id: str, max_results: int = None) -> list[str]:
        """Fetch all video IDs from a playlist."""
        video_ids = []
        next_page_token = None
        
        while True:
            self._rate_limit()
            request = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get("items", []):
                video_id = item.get("contentDetails", {}).get("videoId")
                if video_id:
                    video_ids.append(video_id)
                    if max_results and len(video_ids) >= max_results:
                        return video_ids
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        
        return video_ids
    
    def fetch_videos(self, video_ids: list[str]) -> list[dict]:
        """Fetch detailed metadata for videos (handles batching)."""
        all_videos = []
        
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            self._rate_limit()
            
            request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics,status,topicDetails",
                id=",".join(batch)
            )
            response = request.execute()
            
            for item in response.get("items", []):
                video = self._parse_video(item)
                all_videos.append(video)
        
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
        
        return chapters
    
    def fetch_comments(self, video_id: str, since: datetime = None, max_results: int = 500) -> list[dict]:
        """Fetch comments for a video, optionally only newer than `since`."""
        comments = []
        next_page_token = None
        
        try:
            while len(comments) < max_results:
                self._rate_limit()
                request = self.youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=min(100, max_results - len(comments)),
                    pageToken=next_page_token,
                    order="time",  # Most recent first for incremental
                    textFormat="plainText"
                )
                response = request.execute()
                
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
                    
                    # Get replies
                    if "replies" in item:
                        for reply in item["replies"]["comments"]:
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
                
                if found_old:
                    break
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
        except HttpError as e:
            if e.resp.status == 403:
                # Comments disabled
                return []
            raise
        
        return comments
    
    def fetch_playlists(self, channel_id: str) -> list[dict]:
        """Fetch all playlists for a channel."""
        playlists = []
        next_page_token = None
        
        while True:
            self._rate_limit()
            request = self.youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
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
        
        return playlists


class TranscriptFetcher:
    """Handles fetching video transcripts."""
    
    @staticmethod
    def fetch(video_id: str, language: str = "en") -> Optional[dict]:
        """Fetch transcript using best available method."""
        if TRANSCRIPT_API_AVAILABLE:
            result = TranscriptFetcher._fetch_with_api(video_id, language)
            if result and result.get("available"):
                return result
        
        return TranscriptFetcher._fetch_fallback(video_id)
    
    @staticmethod
    def _fetch_with_api(video_id: str, language: str = "en") -> Optional[dict]:
        """Fetch transcript using youtube-transcript-api library."""
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            transcript = None
            transcript_info = {}
            
            # Try manually created English transcript
            try:
                transcript = transcript_list.find_manually_created_transcript([language, 'en', 'en-US', 'en-GB'])
                transcript_info = {
                    "transcript_type": "manual",
                    "language": transcript.language,
                    "language_code": transcript.language_code
                }
            except NoTranscriptFound:
                pass
            
            # Fall back to auto-generated
            if not transcript:
                try:
                    transcript = transcript_list.find_generated_transcript([language, 'en', 'en-US', 'en-GB'])
                    transcript_info = {
                        "transcript_type": "auto-generated",
                        "language": transcript.language,
                        "language_code": transcript.language_code
                    }
                except NoTranscriptFound:
                    pass
            
            # Try translating to English
            if not transcript:
                try:
                    for t in transcript_list:
                        if t.is_translatable:
                            transcript = t.translate('en')
                            transcript_info = {
                                "transcript_type": "translated",
                                "language": "English",
                                "language_code": "en"
                            }
                            break
                except Exception:
                    pass
            
            if transcript:
                transcript_data = transcript.fetch()
                entries = []
                full_text_parts = []
                
                for entry in transcript_data:
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
                    "full_text": " ".join(full_text_parts)
                }
            
            return {"available": False, "reason": "No English transcript found"}
            
        except TranscriptsDisabled:
            return {"available": False, "reason": "Transcripts disabled"}
        except VideoUnavailable:
            return {"available": False, "reason": "Video unavailable"}
        except Exception as e:
            return {"available": False, "reason": str(e)}
    
    @staticmethod
    def _fetch_fallback(video_id: str) -> Optional[dict]:
        """Fallback method without youtube-transcript-api."""
        try:
            watch_url = f"https://www.youtube.com/watch?v={video_id}"
            response = requests.get(watch_url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=10)
            
            if response.status_code != 200:
                return {"available": False, "reason": "Could not fetch video page"}
            
            caption_pattern = r'"captions":.*?"captionTracks":\[(.*?)\]'
            match = re.search(caption_pattern, response.text)
            
            if not match:
                return {"available": False, "reason": "No captions found"}
            
            caption_tracks = match.group(1)
            
            # Find English caption track
            en_pattern = r'"baseUrl":"([^"]+)"[^}]*"languageCode":"(en[^"]*)"'
            en_match = re.search(en_pattern, caption_tracks)
            
            if not en_match:
                any_pattern = r'"baseUrl":"([^"]+)"'
                any_match = re.search(any_pattern, caption_tracks)
                if not any_match:
                    return {"available": False, "reason": "No English captions found"}
                caption_url = any_match.group(1).replace("\\u0026", "&")
            else:
                caption_url = en_match.group(1).replace("\\u0026", "&")
            
            caption_response = requests.get(caption_url, timeout=10)
            if caption_response.status_code != 200:
                return {"available": False, "reason": "Could not fetch captions"}
            
            root = ET.fromstring(caption_response.text)
            
            entries = []
            full_text_parts = []
            
            for text_elem in root.findall(".//text"):
                start = float(text_elem.get("start", 0))
                duration = float(text_elem.get("dur", 0))
                text = text_elem.text or ""
                text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                text = text.replace("&#39;", "'").replace("&quot;", '"')
                
                entries.append({
                    "start": start,
                    "duration": duration,
                    "end": start + duration,
                    "text": text
                })
                full_text_parts.append(text)
            
            return {
                "available": True,
                "transcript_type": "fetched",
                "language": "English",
                "language_code": "en",
                "entries": entries,
                "full_text": " ".join(full_text_parts)
            }
            
        except Exception as e:
            return {"available": False, "reason": str(e)}
