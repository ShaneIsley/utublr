"""
YouTube API quota tracking and management.

YouTube Data API v3 has a daily quota of 10,000 units (default).
This module tracks usage and prevents exceeding limits.

Quota state is persisted to the database to track usage across runs.
Thread-safe for parallel channel processing with dedicated connection.

Configuration is loaded from config/channels.yaml settings section or environment variables.
"""

import json
import os
import threading
from datetime import datetime, date
from typing import Optional

from config import get_config
from logger import get_logger
from database import get_connection as get_db_connection, is_postgres

log = get_logger("quota")


class QuotaExhaustedError(Exception):
    """Raised when API quota is exhausted or insufficient for operation."""
    pass


class QuotaTracker:
    """
    Track YouTube API quota usage across runs.

    Persists quota usage to database so we can track across multiple runs in a day.
    Uses a dedicated database connection to avoid conflicts with worker threads.

    Features:
    - Thread-safe quota tracking with minimal lock contention
    - Auto-checkpoint every N quota units for crash safety
    - Explicit flush() for phase transitions
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
        warn_threshold: float = None,
        abort_threshold: float = None,
        checkpoint_threshold: int = None,
    ):
        """
        Initialize quota tracker with dedicated database connection.

        Args:
            daily_limit: Daily quota limit (default: from config or 10000)
            warn_threshold: Fraction of quota at which to warn (default: from config or 0.8)
            abort_threshold: Fraction of quota at which to abort (default: from config or 0.95)
            checkpoint_threshold: Auto-save quota state every N units spent (default: from config or 500)
        """
        cfg = get_config()
        self.daily_limit = daily_limit if daily_limit is not None else cfg.quota_limit
        self.warn_threshold = warn_threshold if warn_threshold is not None else cfg.quota_warn_threshold
        self.abort_threshold = abort_threshold if abort_threshold is not None else cfg.quota_abort_threshold
        self._checkpoint_threshold = checkpoint_threshold if checkpoint_threshold is not None else cfg.quota_checkpoint_threshold

        self.today = date.today().isoformat()
        self.used = 0
        self.operations = {}  # Track by operation type
        self.session_used = 0  # Just this run
        self.session_start = datetime.now()

        # Thread-safety: separate locks for quota state and DB operations
        self._lock = threading.Lock()  # Protects quota state (fast operations)
        self._db_lock = threading.Lock()  # Serializes DB saves (slow operations)

        # Checkpoint tracking
        self._dirty = False  # Has unsaved changes
        self._since_checkpoint = 0  # Quota units since last save

        # Load initial state (creates temporary connection)
        self._load_state()

        log.info(f"Quota tracker initialized: limit={self.daily_limit}, used_today={self.used}")
        if self.used > 0:
            log.info(f"Resumed from previous runs: {self.used} units already used today")
        log.debug(f"Thresholds: warn={self.warn_threshold}, abort={self.abort_threshold}, "
                  f"checkpoint={self._checkpoint_threshold}")

    def _get_connection(self):
        """
        Create a fresh database connection for quota operations.

        Uses the database module's get_connection() which respects the
        configured backend (PostgreSQL or Turso/libsql).
        """
        return get_db_connection()

    def _load_state(self):
        """Load persisted quota state from database."""
        try:
            conn = self._get_connection()
            result = conn.execute("""
                SELECT used, operations FROM quota_usage WHERE date = ?
            """, (self.today,)).fetchone()

            if result:
                self.used = result[0]
                self.operations = json.loads(result[1]) if result[1] else {}
                log.debug(f"Loaded quota state from database: {self.used} used today")
            else:
                log.debug(f"No quota state for {self.today}, starting fresh")
        except Exception as e:
            log.warning(f"Could not load quota state: {e}")

    def _save_state(self):
        """
        Persist quota state to database.

        Creates a fresh connection for thread-safety - libsql's C library
        isn't thread-safe when sharing connections across threads.
        """
        try:
            # Get current state snapshot (with quota lock)
            with self._lock:
                used = self.used
                operations = self.operations.copy()

            # Create fresh connection for this save (thread-safe)
            conn = self._get_connection()
            conn.execute("""
                INSERT INTO quota_usage (date, used, operations, last_updated)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    used = excluded.used,
                    operations = excluded.operations,
                    last_updated = excluded.last_updated
            """, (self.today, used, json.dumps(operations), datetime.now().isoformat()))
            conn.commit()
            log.debug(f"Saved quota state to database: {used} used")
        except Exception as e:
            log.warning(f"Could not save quota state: {e}")
    
    def reset(self):
        """Reset quota counter to 0. Use if quota tracking was corrupted."""
        with self._lock:
            self.used = 0
            self.session_used = 0
            self.operations = {}
            self._dirty = True
            self._since_checkpoint = 0
        self.flush()
        log.info(f"Quota reset to 0 for {self.today}")
    
    def use(self, operation: str, count: int = 1) -> int:
        """
        Record quota usage for an operation. Thread-safe with auto-checkpoint.

        Fast path: just update counters and mark dirty.
        Auto-checkpoint triggers save when threshold reached.

        Args:
            operation: API operation name (e.g., 'videos.list')
            count: Number of API calls made

        Returns:
            Cost in quota units
        """
        cost = self.COSTS.get(operation, 1) * count
        should_checkpoint = False

        with self._lock:
            self.used += cost
            self.session_used += cost
            self.operations[operation] = self.operations.get(operation, 0) + cost
            self._since_checkpoint += cost
            self._dirty = True
            current_used = self.used

            # Check if we should auto-checkpoint
            if self._since_checkpoint >= self._checkpoint_threshold:
                should_checkpoint = True

        log.debug(f"Quota: +{cost} for {operation} x{count} (total: {current_used}/{self.daily_limit})")

        # Auto-checkpoint for large channels (outside quota lock to reduce contention)
        if should_checkpoint:
            self.flush()

        self._check_thresholds()

        return cost

    def flush(self):
        """
        Explicitly save quota state to database.

        Call this at phase transitions (after videos, after comments, etc.)
        or at channel completion. Thread-safe.
        """
        with self._db_lock:
            with self._lock:
                if not self._dirty:
                    return  # Nothing to save
                self._dirty = False
                self._since_checkpoint = 0

            self._save_state()
    
    def _check_thresholds(self):
        """Check if we've hit warning or abort thresholds."""
        usage_fraction = self.used / self.daily_limit
        
        if usage_fraction >= self.abort_threshold:
            log.error(f"QUOTA CRITICAL: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
        elif usage_fraction >= self.warn_threshold:
            log.warning(f"QUOTA WARNING: {self.used}/{self.daily_limit} ({usage_fraction:.1%})")
    
    def remaining(self) -> int:
        """Get remaining quota units. Thread-safe."""
        with self._lock:
            return max(0, self.daily_limit - self.used)

    def used_fraction(self) -> float:
        """Get fraction of quota used. Thread-safe."""
        with self._lock:
            return self.used / self.daily_limit

    def can_afford(self, operation: str, count: int = 1) -> bool:
        """Check if we can afford an operation without exceeding abort threshold. Thread-safe."""
        cost = self.COSTS.get(operation, 1) * count
        with self._lock:
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
