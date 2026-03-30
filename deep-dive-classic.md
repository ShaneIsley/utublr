# utublr: A Production ETL Pipeline for YouTube Analytics

*2026-03-30T01:20:22Z by Showboat 0.6.1*
<!-- showboat-id: 2b6bcd7e-aa4f-4f50-a51c-9abaf365f7e2 -->

## The Problem: YouTube Data Is a Moving Target

YouTube channels are living things. Videos get published, deleted, made private. View counts climb. Comments pile up. To understand how a channel evolves, you need to track changes over time.

**utublr** is a production ETL system that pulls metadata from the YouTube Data API, transforms it into a time-series schema, and writes it to Turso (cloud SQLite at the edge). It runs unattended on GitHub Actions, four times a day, across 20+ channels.

But the interesting engineering isn't the happy path. It's everything that can go wrong.

## Data Flow: API → Transform → Cloud DB

The schema captures **dimension tables** (channels, videos) that store the latest attributes, and **time-series tables** (channel_stats, video_stats) that append a new row every fetch — giving us change-over-time for free.

```bash
sed -n '670,683p' scripts/database.py
```

```output
        CREATE TABLE IF NOT EXISTS video_stats (
            video_id TEXT,
            fetched_at TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            PRIMARY KEY (video_id, fetched_at)
        )
    """)
    
    # Chapters
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chapters (
            video_id TEXT,
```

The composite primary key `(video_id, fetched_at)` means each fetch appends a snapshot — no UPDATEs on the stats table. This lets us answer "how fast did views grow in the first 48 hours?" across 14 tables covering channels, videos, comments, transcripts, chapters, playlists, and four operational tables: **fetch_log**, **fetch_progress** (resumable checkpoints), **quota_usage** (daily API budget), and **video_comment_summary** (a denormalized cache avoiding expensive COUNTs).

## Incremental Updates: Knowing What Changed

YouTube's daily API quota is 10,000 units. A naive full-fetch of 20 channels would burn through that in one run. The first line of defense is the **early-exit optimization**:

```bash
sed -n '426,432p' scripts/fetch.py
```

```output
        youtube_video_count = channel_data['statistics']['video_count']
        existing_video_ids = get_existing_video_ids(conn, channel_id)
        stored_video_count = len(existing_video_ids)
        
        # For incremental fetches, skip expensive video discovery if counts match
        if not backfill and youtube_video_count == stored_video_count and stored_video_count > 0:
            log.info(f"Video count unchanged ({youtube_video_count}), skipping video discovery")
```

If video counts match, the system skips video discovery entirely — saving 100 units per search API call — but still checks for stale comments. The second key insight is **tiered refresh rates**. Not all videos need the same update frequency:

```bash
sed -n '325,340p' scripts/fetch.py
```

```output
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
```

A video less than 48 hours old gets stats every run; one older than 30 days, once a day. Quota is spent where data is actually changing.

## Checkpointing: Surviving Interruptions

A fetch run processing thousands of videos can take 30+ minutes. GitHub Actions has timeouts. Networks drop. The system needs to pick up where it left off via the `fetch_progress` table:

```bash
sed -n '754,763p' scripts/database.py
```

```output
        CREATE TABLE IF NOT EXISTS fetch_progress (
            channel_id TEXT,
            fetch_id INTEGER,
            operation TEXT,
            processed_ids TEXT,
            total_count INTEGER,
            last_updated TEXT,
            PRIMARY KEY (channel_id, fetch_id, operation)
        )
    """)
```

The `processed_ids` column stores a JSON-serialized set of video IDs already handled. After each batch, the system checkpoints:

```bash
sed -n '642,650p' scripts/fetch.py
```

```output
                        # Batch DB writes - single network round-trip per operation
                        upsert_videos_batch(conn, videos, commit=False)
                        if chapters_by_video:
                            upsert_chapters_batch(conn, chapters_by_video, commit=False)
                        conn.commit()

                        # Checkpoint: save progress
                        save_progress(conn, channel_id, fetch_id, 'videos',
                                     processed_ids, len(video_ids_to_fetch))
```

On resume, the checkpoint filters out already-processed videos:

```bash
sed -n '760,765p' scripts/fetch.py
```

```output
                # Check for progress (resume from checkpoint)
                comment_progress = get_progress(conn, channel_id, fetch_id, 'comments')
                if comment_progress:
                    processed_comment_video_ids = comment_progress['processed_ids']
                    videos_for_comments = [v for v in videos_for_comments 
                                          if v not in processed_comment_video_ids]
```

This is idempotent by design. Running the same fetch twice won't duplicate data — upserts and set-based deduplication ensure that. Progress is cleared only on successful completion, so a crash means the next run resumes from the last checkpoint.

## Quota Tracking: Living Within YouTube's Budget

The quota tracker is thread-safe (comment fetching is parallelized) and auto-checkpoints to the database every 500 units to survive crashes:

```bash
sed -n '44,55p' scripts/quota.py
```

```output
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
```

Note the 100x cost difference between `search.list` and `videos.list` — this is why the incremental logic avoids search whenever possible. The core of the tracker uses a two-lock design for concurrency:

```bash
sed -n '181,204p' scripts/quota.py
```

```output
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
```

`_lock` is the fast path for counter updates (held briefly, even from parallel comment threads). The `should_checkpoint` flag is evaluated inside the lock but `flush()` happens outside it — using a separate `_db_lock` for database persistence. A thread recording quota usage never blocks on a database write. Quota state persists across runs, so a crash at 6,000 units means the next run picks up there.

## Deletion Detection: Absence as Signal

How do you know a video was *deleted* versus just not returned by a paginated API? The system only trusts deletion signals from the playlist API, which returns complete upload lists:

```bash
sed -n '566,576p' scripts/fetch.py
```

```output
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
```

Set subtraction (`existing - current`) reliably identifies removals in playlist mode. The search API returns relevance-sorted pages that may omit older videos, making absence ambiguous — so the system wisely refuses to draw conclusions from it. Deleted videos are soft-deleted (`privacy_status='deleted'`), with permanent purging only after 30 days.

## CI/CD: GitHub Actions as a Scheduler

No dedicated infrastructure — the pipeline runs on GitHub Actions with cron:

```bash
sed -n '54,71p' .github/workflows/fetch.yml
```

```output
  # Scheduled runs - 4 times daily
  schedule:
    - cron: '0 0 * * *'   # Midnight UTC
    - cron: '0 6 * * *'   # 6 AM UTC
    - cron: '0 12 * * *'  # Noon UTC
    - cron: '0 18 * * *'  # 6 PM UTC

env:
  PYTHON_VERSION: '3.11'

jobs:
  fetch:
    runs-on: ubuntu-latest
    
    # Prevent concurrent runs to avoid database conflicts
    concurrency:
      group: youtube-fetch
      cancel-in-progress: false
```

`cancel-in-progress: false` is critical: if a run is still going when the next cron fires, the new run *queues* rather than cancelling the in-flight one. This prevents data corruption from overlapping writes while ensuring no scheduled run is dropped.

The workflow exposes every operational knob as a manual `workflow_dispatch` input — single-channel runs, backfill mode, quota resets, log levels — turning GitHub Actions into an operational control plane. **Dry-run mode** previews what would happen without spending quota:

```bash
sed -n '927,940p' scripts/fetch.py
```

```output
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
    
```

Dry-run uses only 2 API units per channel (resolve ID + get counts), then estimates the rest from database state — safe to run repeatedly for monitoring.

## Retry and Backoff: Designed In, Not Bolted On

The YouTube API client and database layer have independent retry logic because they fail in different ways. The API decorator handles transient HTTP errors with exponential backoff:

```bash
sed -n '88,100p' scripts/youtube_api.py
```

```output
            last_exception = None

            for attempt in range(_max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except HttpError as e:
                    status_code = e.resp.status if hasattr(e, 'resp') else None

                    if status_code in RETRYABLE_STATUS_CODES:
                        last_exception = e
                        if attempt < _max_retries:
                            delay = min(_base_delay * (exponential_base ** attempt), _max_delay)
```

```bash
sed -n '103,114p' scripts/youtube_api.py
```

```output
                            retry_after = e.resp.get('retry-after') if hasattr(e, 'resp') else None
                            if retry_after:
                                try:
                                    delay = max(delay, float(retry_after))
                                except ValueError:
                                    pass

                            log.warning(f"HTTP {status_code} error, retrying in {delay:.1f}s "
                                       f"(attempt {attempt + 1}/{_max_retries + 1}): {e}")
                            time.sleep(delay)
                            continue
                    else:
```

The `Retry-After` header is used as a floor — the exponential backoff may exceed it, and the system takes whichever is larger. The decorator also catches `AttributeError` because httplib2 (Google's API client) has thread-safety issues where a `NoneType` sneaks in during concurrent access — treated as transient and retried.

The **database layer** has a three-tier error classification for Turso's unique failure modes:

```bash
sed -n '40,65p' scripts/database.py
```

```output
# Turso-specific error patterns that are retryable
RETRYABLE_ERROR_PATTERNS = [
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    "Connection reset",
    "Connection refused",
    "Connection timed out",
    "Temporary failure",
    "Too many requests",
    "SQLITE_BUSY",
    "database is locked",
    # Turso/Hrana stream errors - require connection refresh
    "stream not found",
    "Stream already in use",
]

# Patterns that indicate the connection needs to be recreated
CONNECTION_REFRESH_PATTERNS = [
    "stream not found",
    "Stream already in use",
]

# Patterns that indicate JWT token expiry or auth issues (not retryable)
JWT_TOKEN_ERROR_PATTERNS = [
    "token expired",
```

The retry handler uses `getattr(self._conn, method_name)` to get a *fresh* method reference on each attempt — so after `_refresh_connection()` replaces the underlying connection, retries call the new one, not the dead one:

```bash
sed -n '236,244p' scripts/database.py
```

```output
                elif needs_connection_refresh(e):
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (exp_base ** attempt), max_delay)
                        log.warning(f"Stream error, refreshing connection in {delay:.1f}s "
                                   f"(attempt {attempt + 1}/{max_retries + 1}): {e}")
                        time.sleep(delay)
                        self._refresh_connection()
                        continue
```

Three error categories, three recovery strategies: **JWT token expiry** halts with clear remediation steps. **Stream errors** retry with connection refresh (the HTTP/2 stream is dead). **Transient errors** (502, SQLITE_BUSY) retry on the existing connection. This prevents wasting retry attempts on the wrong strategy.

## Putting It All Together

The system processes channels sequentially — Turso's libSQL driver isn't thread-safe for multiple channels — but parallelizes *within* each channel for comment fetching:

```bash
sed -n '1314,1318p' scripts/fetch.py
```

```output
    # Note: Comment workers within a single channel are still parallel and safe.
    if channel_workers > 1:
        log.warning("Parallel channel workers disabled due to httplib2/SSL thread-safety issues")
        log.warning("Processing channels sequentially. Comment fetching remains parallel.")
        channel_workers = 1
```

A pragmatic decision: rather than fighting httplib2's thread-safety limitation, the system works *with* the constraint. The comment is honest about *why*, which matters for maintenance.

## What This Architecture Gets Right

Production ETL isn't about a `for` loop that calls an API and inserts rows. It's about:

- **Idempotency**: Every operation can be re-run safely. Upserts, checkpoint-based resumption, and set-based deduplication ensure no duplicate data.
- **Incremental processing**: Tiered refresh rates and early-exit checks keep quota usage proportional to *change*, not *volume*.
- **Failure recovery**: Checkpoints persist to the database, not memory. Quota state survives crashes. Stream errors trigger connection refresh, not just retry.
- **Operational visibility**: Dry-run mode, configurable log levels, fetch history, and GitHub Actions summaries let you diagnose problems without deploying new code.
- **Honest constraints**: Where the system can't do something safely (parallel channels, deletion detection via search), it says so and degrades gracefully rather than producing silent data quality issues.

The system tracks 20+ channels across 4 daily runs while staying well under YouTube's 10,000-unit daily quota — because every API call is justified, every failure is anticipated, and every interruption is recoverable.
