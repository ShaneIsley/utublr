"""
YouTube API quota tracking and management.

YouTube Data API v3 has a daily quota of 10,000 units (default).
This module tracks usage and prevents exceeding limits.

Quota state is persisted to the database to track usage across runs.
Thread-safe for parallel channel processing with dedicated connection.
"""

import json
import os
import threading
from datetime import datetime, date
from typing import Optional

from logger import get_logger

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
        warn_threshold: float = 0.8,  # Warn at 80% usage
        abort_threshold: float = 0.95,  # Abort at 95% usage
        checkpoint_threshold: int = 500,  # Auto-save every N quota units
    ):
        """
        Initialize quota tracker with dedicated database connection.

        Args:
            daily_limit: Daily quota limit (default: YOUTUBE_QUOTA_LIMIT env or 10000)
            warn_threshold: Fraction of quota at which to warn
            abort_threshold: Fraction of quota at which to abort
            checkpoint_threshold: Auto-save quota state every N units spent
        """
        self.daily_limit = daily_limit or int(os.environ.get("YOUTUBE_QUOTA_LIMIT", 10000))
        self.warn_threshold = warn_threshold
        self.abort_threshold = abort_threshold
        self._checkpoint_threshold = checkpoint_threshold

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

        Creates a new connection each time to avoid thread-safety issues
        with libsql's C library when multiple threads access the same connection.
        """
        import libsql
        from database import is_jwt_token_error

        url = os.environ.get("TURSO_DATABASE_URL", "file:data/youtube.db")
        auth_token = os.environ.get("TURSO_AUTH_TOKEN", "")

        try:
            if url.startswith("libsql://") or url.startswith("https://"):
                return libsql.connect(database=url, auth_token=auth_token)
            else:
                if url.startswith("file:"):
                    filepath = url[5:]
                    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
                return libsql.connect(database=url)
        except Exception as e:
            # Check for JWT token errors
            if is_jwt_token_error(e):
                log.error(f"Failed to connect to Turso database: {e}")
                log.error("")
                log.error("=" * 70)
                log.error("ERROR: Turso database JWT token has expired!")
                log.error("=" * 70)
                log.error("")
                log.error("The TURSO_AUTH_TOKEN environment variable contains an expired token.")
                log.error("")
                log.error("To fix this issue:")
                log.error("1. Generate a new token using: turso db tokens create <database-name>")
                log.error("2. Update the TURSO_AUTH_TOKEN secret in GitHub Actions settings")
                log.error("3. Or update the TURSO_AUTH_TOKEN environment variable if running locally")
                log.error("")
                log.error("For more information, see: https://docs.turso.tech/cli/db/tokens/create")
                log.error("=" * 70)
                raise RuntimeError(
                    "Turso JWT token has expired. Please generate a new token using "
                    "'turso db tokens create <database-name>' and update the TURSO_AUTH_TOKEN "
                    "environment variable or GitHub Actions secret."
                ) from e
            else:
                # Re-raise other connection errors
                raise

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
