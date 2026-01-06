"""
YouTube API quota tracking and management.

YouTube Data API v3 has a daily quota of 10,000 units (default).
This module tracks usage and prevents exceeding limits.

Quota state is persisted to the database to track usage across runs.
"""

import json
import os
from datetime import datetime, date
from typing import Optional

from database import get_quota_usage, save_quota_usage
from logger import get_logger

log = get_logger("quota")


class QuotaExhaustedError(Exception):
    """Raised when API quota is exhausted or insufficient for operation."""
    pass


class QuotaTracker:
    """
    Track YouTube API quota usage across runs.
    
    Persists quota usage to database so we can track across multiple runs in a day.
    """
    
    # API operation costs (units)
    # See: https://developers.google.com/youtube/v3/determine_quota_cost
    COSTS = {
        'channels.list': 1,
        'playlists.list': 1,
        'playlistItems.list': 1,
        'videos.list': 1,
        'commentThreads.list': 1,
        'comments.list': 1,
        'search.list': 100,  # Very expensive - avoid!
        'captions.list': 50,
    }
    
    def __init__(
        self, 
        daily_limit: int = None,
        conn = None,
        warn_threshold: float = 0.8,  # Warn at 80% usage
        abort_threshold: float = 0.95  # Abort at 95% usage
    ):
        """
        Initialize quota tracker.
        
        Args:
            daily_limit: Daily quota limit (default: YOUTUBE_QUOTA_LIMIT env or 10000)
            conn: Database connection for persistence (required for cross-run tracking)
            warn_threshold: Fraction of quota at which to warn
            abort_threshold: Fraction of quota at which to abort
        """
        self.daily_limit = daily_limit or int(os.environ.get("YOUTUBE_QUOTA_LIMIT", 10000))
        self.conn = conn
        self.warn_threshold = warn_threshold
        self.abort_threshold = abort_threshold
        
        self.today = date.today().isoformat()
        self.used = 0
        self.operations = {}  # Track by operation type
        self.session_used = 0  # Just this run
        self.session_start = datetime.now()
        
        self._load_state()
        
        log.info(f"Quota tracker initialized: limit={self.daily_limit}, used_today={self.used}")
        if self.used > 0:
            log.info(f"Resumed from previous runs: {self.used} units already used today")
        log.debug(f"Thresholds: warn={self.warn_threshold}, abort={self.abort_threshold}")
    
    def _load_state(self):
        """Load persisted quota state from database."""
        if not self.conn:
            log.debug("No database connection, quota state won't persist")
            return

        try:
            state = get_quota_usage(self.conn, self.today)
            
            if state:
                self.used = state.get('used', 0)
                self.operations = state.get('operations', {})
                log.debug(f"Loaded quota state from database: {self.used} used today")
            else:
                log.debug(f"No quota state for {self.today}, starting fresh")
        except Exception as e:
            log.warning(f"Could not load quota state: {e}")
    
    def _save_state(self):
        """Persist quota state to database."""
        if not self.conn:
            return

        try:
            save_quota_usage(self.conn, self.today, self.used, self.operations)
            log.debug(f"Saved quota state to database: {self.used} used")
        except Exception as e:
            log.warning(f"Could not save quota state: {e}")
    
    def reset(self):
        """Reset quota counter to 0. Use if quota tracking was corrupted."""
        self.used = 0
        self.session_used = 0
        self.operations = {}
        self._save_state()
        log.info(f"Quota reset to 0 for {self.today}")
    
    def use(self, operation: str, count: int = 1) -> int:
        """
        Record quota usage for an operation.
        
        Args:
            operation: API operation name (e.g., 'videos.list')
            count: Number of API calls made
        
        Returns:
            Cost in quota units
        """
        cost = self.COSTS.get(operation, 1) * count
        self.used += cost
        self.session_used += cost
        self.operations[operation] = self.operations.get(operation, 0) + cost
        
        log.debug(f"Quota: +{cost} for {operation} x{count} (total: {self.used}/{self.daily_limit})")
        
        self._save_state()
        self._check_thresholds()
        
        return cost
    
    def _check_thresholds(self):
        """Check if we've hit warning or abort thresholds."""
        usage_fraction = self.used / self.daily_limit
        
        if usage_fraction >= self.abort_threshold:
            log.error(f"QUOTA CRITICAL: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
        elif usage_fraction >= self.warn_threshold:
            log.warning(f"QUOTA WARNING: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
    
    def remaining(self) -> int:
        """Get remaining quota units."""
        return max(0, self.daily_limit - self.used)
    
    def used_fraction(self) -> float:
        """Get fraction of quota used."""
        return self.used / self.daily_limit
    
    def can_afford(self, operation: str, count: int = 1) -> bool:
        """Check if we can afford an operation without exceeding abort threshold."""
        cost = self.COSTS.get(operation, 1) * count
        projected = self.used + cost
        return projected <= (self.daily_limit * self.abort_threshold)
    
    def estimate_channel_cost(
        self, 
        video_count: int, 
        fetch_comments: bool = True,
        fetch_transcripts: bool = True,
        max_comments_per_video: int = 100
    ) -> dict:
        """
        Estimate quota cost to fully fetch a channel.
        
        Returns dict with breakdown by operation.
        """
        estimates = {
            'channels.list': 1,
            'playlists.list': 2,  # Assume 2 pages
            'playlistItems.list': (video_count // 50) + 1,
            'videos.list': (video_count // 50) + 1,
        }
        
        if fetch_comments:
            # Worst case: every video needs comments fetched
            # Each video might need multiple pages
            pages_per_video = max(1, max_comments_per_video // 100)
            estimates['commentThreads.list'] = video_count * pages_per_video
        
        # Transcripts don't use YouTube API quota (scraped separately)
        
        total = sum(estimates.values())
        estimates['total'] = total
        
        log.debug(f"Estimated quota for {video_count} videos: {estimates}")
        
        return estimates
    
    def check_or_abort(self, needed: int, operation: str = "operation"):
        """
        Check if we have enough quota, raise exception if not.
        
        Args:
            needed: Quota units needed
            operation: Description for error message
        
        Raises:
            QuotaExhaustedError: If insufficient quota
        """
        if self.remaining() < needed:
            raise QuotaExhaustedError(
                f"Insufficient quota for {operation}: need {needed}, have {self.remaining()}"
            )
        
        projected_usage = (self.used + needed) / self.daily_limit
        if projected_usage > self.abort_threshold:
            raise QuotaExhaustedError(
                f"Would exceed abort threshold ({self.abort_threshold:.0%}) for {operation}"
            )
    
    def get_summary(self) -> dict:
        """Get summary of quota usage for logging/reporting."""
        return {
            'date': self.today,
            'used': self.used,
            'remaining': self.remaining(),
            'limit': self.daily_limit,
            'used_fraction': self.used_fraction(),
            'session_used': self.session_used,
            'session_duration': str(datetime.now() - self.session_start),
            'by_operation': self.operations.copy()
        }
    
    def log_summary(self):
        """Log a summary of quota usage."""
        summary = self.get_summary()
        log.info(f"Quota summary: {summary['used']}/{summary['limit']} "
                 f"({summary['used_fraction']:.1%}), session: {summary['session_used']}")
        for op, cost in sorted(summary['by_operation'].items(), key=lambda x: -x[1]):
            log.debug(f"  {op}: {cost} units")
