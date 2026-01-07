# YouTube Channel Analytics Tracker

A production-ready system for tracking YouTube channel metadata over time using Turso (cloud SQLite) for persistent storage.

## Features

- **Multi-channel tracking** - Monitor dozens of channels from a single config file
- **Smart incremental updates** - Only fetches new/changed data to minimize API usage
- **Turso cloud database** - No more artifact uploads; data persists reliably
- **Quota tracking** - Monitors API usage across runs, warns before exhaustion
- **Checkpointing** - Resume interrupted fetches from where they left off
- **Retry with backoff** - Handles transient API/database errors gracefully
- **Dry-run mode** - Preview what would be fetched without using quota
- **Deletion detection** - Tracks when videos are removed from channels
- **Detailed logging** - DEBUG logs to file for troubleshooting

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Turso Database

```bash
# Install Turso CLI
curl -sSfL https://get.tur.so/install.sh | bash

# Create database
turso db create youtube-tracker
turso db show youtube-tracker --url    # Save this URL
turso db tokens create youtube-tracker  # Save this token
```

### 3. Set Environment Variables

```bash
export YOUTUBE_API_KEY="your-youtube-api-key"
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

### 4. Create Channel Config

```bash
cp config/channels.example.yaml config/channels.yaml
# Edit with your channels
```

### 5. Run

```bash
cd scripts

# Dry run first to see what would happen
python fetch.py --config ../config/channels.yaml --dry-run

# Actual fetch
python fetch.py --config ../config/channels.yaml --backfill

# Fetch transcripts locally (YouTube blocks cloud IPs)
python fetch_transcripts.py --channel @YourChannel
```

## Scripts

### fetch.py - Main Data Fetcher (runs in CI/cloud)

Fetches channel metadata, video info, stats, and comments via YouTube API.

```bash
# Basic usage
python fetch.py --channel @GoogleDevelopers
python fetch.py --config ../config/channels.yaml

# Dry run (preview without fetching)
python fetch.py --config ../config/channels.yaml --dry-run

# Backfill mode (fetch everything, ignore incremental)
python fetch.py --channel @GoogleDevelopers --backfill

# With limits
python fetch.py --channel @GoogleDevelopers \
    --max-videos 500 \
    --max-video-age 90 \
    --max-comments 100 \
    --max-replies 10

# Skip comments to save quota
python fetch.py --config ../config/channels.yaml --skip-comments

# Export to CSV
python fetch.py --export
```

### fetch_transcripts.py - Transcript Fetcher (run locally)

Fetches video transcripts. **Must be run locally** because YouTube blocks 
transcript requests from cloud/CI IP addresses.

```bash
# Fetch all missing transcripts
python fetch_transcripts.py

# Fetch for specific channel
python fetch_transcripts.py --channel @samwitteveenai
python fetch_transcripts.py --channel UC55ODQSvARtgSyc8ThfiepQ

# Fetch specific videos
python fetch_transcripts.py --video VIDEO_ID1 VIDEO_ID2

# Limit number to fetch
python fetch_transcripts.py --limit 100

# Slower rate limiting (default: 1 req/sec)
python fetch_transcripts.py --delay 2.0

# Test mode (don't save to database)
python fetch_transcripts.py --dry-run --limit 10
```

### analyse.py - Reports and Analysis

```bash
# List available reports
python analyse.py --list-reports

# Run preset reports
python analyse.py --report summary
python analyse.py --report channels
python analyse.py --report growth
python analyse.py --report top-videos

# Custom SQL
python analyse.py --sql "SELECT title, view_count FROM videos ORDER BY view_count DESC LIMIT 10"

# Export to CSV
python analyse.py --report growth --output growth.csv
```

### test_transcript.py - Transcript Debugging

```bash
# Test specific video
python test_transcript.py VIDEO_ID

# Test videos from a channel
python test_transcript.py --channel @samwitteveenai --limit 20

# Quick summary only
python test_transcript.py --channel @samwitteveenai --limit 50 --quiet
```

## Command Line Options

### fetch.py

| Option | Default | Description |
|--------|---------|-------------|
| `--channel` | - | Single channel ID, handle, or URL |
| `--config` | - | Path to channels.yaml config file |
| `--export` | - | Export database to CSV files |
| `--dry-run` | false | Preview what would be fetched |
| `--backfill` | false | Ignore incremental, fetch everything |
| `--max-videos` | unlimited | Max videos per channel |
| `--max-video-age` | unlimited | Only fetch videos within N days |
| `--max-comments` | 100 | Max comments per video |
| `--max-replies` | 10 | Max replies per comment |
| `--max-comment-videos` | 200 | Max videos to fetch comments for per run |
| `--video-discovery-mode` | auto | Video discovery: auto, search, or playlist |
| `--comment-workers` | 3 | Parallel workers for comment fetching (1=sequential) |
| `--channel-workers` | 1 | Parallel workers for channel processing (1=sequential) |
| `--batch-size` | 10 | Videos per batch/commit |
| `--stats-update-hours` | 6 | Skip stats if updated within N hours |
| `--comments-update-hours` | 24 | Skip comments if fetched within N hours |
| `--comments-update-hours-new` | 6 | Comment update frequency for new videos |
| `--new-video-days` | 7 | Videos younger than this are "new" |
| `--max-runtime-minutes` | 300 | Stop after N minutes |
| `--quota-limit` | 10000 | Daily API quota limit |
| `--skip-comments` | false | Don't fetch comments |

### fetch_transcripts.py

| Option | Default | Description |
|--------|---------|-------------|
| `--channel` | - | Channel ID or @handle to filter by |
| `--video` | - | Specific video ID(s) to fetch |
| `--limit` | unlimited | Max videos to process |
| `--delay` | 1.0 | Seconds between requests |
| `--dry-run` | false | Don't save to database |
| `--quiet` | false | Only show summary |

## Smart Incremental Fetching

The system minimizes redundant API requests:

| Data Type | Update Strategy | Skip If... |
|-----------|-----------------|------------|
| Channel stats | Every 6 hours | Updated within `--stats-update-hours` |
| Video metadata | Only new videos | Video already in DB |
| Video stats | Stale videos only | Updated within `--stats-update-hours` |
| Comments (old videos) | Every 24 hours | Fetched within `--comments-update-hours` |
| Comments (new videos) | Every 6 hours | Fetched within `--comments-update-hours-new` |
| Transcripts | Manual, local only | Run `fetch_transcripts.py` locally |

## Error Handling

### Retry Logic
All API and database calls retry up to 3-5 times with exponential backoff for:
- HTTP 429 (rate limit)
- HTTP 500/502/503/504 (server errors)
- Database connection errors (Turso 502, stream not found, etc.)

### Checkpointing
Progress is saved after each batch. If a run fails:
1. Videos already processed are saved
2. Next run resumes from where it left off
3. No duplicate work or data loss

### Quota Protection
- Tracks quota usage across runs (persisted to database)
- Auto-checkpoints every 500 quota units for crash safety
- Saves at phase transitions (videos → stats → comments)
- Warns at 80% usage, aborts at 95%
- Estimates cost before fetching each channel

## Logging

Logs are written to `scripts/logs/` with DEBUG level:

```
logs/
├── fetch_20250103_143052.log  # Timestamped log files
└── latest.log                  # Symlink to most recent
```

## GitHub Actions Setup

### 1. Add Repository Secrets

- `YOUTUBE_API_KEY`: Your YouTube Data API key
- `TURSO_DATABASE_URL`: `libsql://your-db.turso.io`
- `TURSO_AUTH_TOKEN`: Your Turso auth token

### 2. Workflow Features

- Runs 4x daily (configurable)
- Manual trigger with options
- Logs uploaded as artifacts
- Summary report in Actions UI

**Note:** Transcripts cannot be fetched in GitHub Actions (YouTube blocks cloud IPs).
Run `fetch_transcripts.py` locally after the automated fetch completes.

## Database Schema

### Core Tables

```sql
channels        -- Channel metadata
channel_stats   -- Subscriber/view counts over time (append-only)
videos          -- Video metadata
video_stats     -- View/like counts over time (append-only)
chapters        -- Video chapters
transcripts     -- Video transcripts (write-once)
comments        -- All comments with threading
playlists       -- Channel playlists
fetch_log       -- Run history
fetch_progress  -- Resume checkpoints
quota_usage     -- API quota tracking
```

## Database Backends

The fetcher supports two database backends:

### Turso (Default)
Cloud-hosted SQLite with edge replication. **Only supports sequential processing** (1 channel at a time).

> ⚠️ **Note:** Turso's libsql-experimental native library has thread-safety issues that cause
> heap corruption when using parallel workers. The fetcher automatically forces `channel_workers=1`
> when using Turso. Use PostgreSQL if you need parallel channel processing.

```bash
export DATABASE_BACKEND=turso  # or omit (default)
export TURSO_DATABASE_URL=libsql://your-db.turso.io
export TURSO_AUTH_TOKEN=your-token
```

### PostgreSQL
Traditional database with excellent concurrency. **Recommended for parallel workers.**

```bash
export DATABASE_BACKEND=postgres
export POSTGRES_URL=postgresql://user:pass@host:5432/dbname
```

PostgreSQL handles concurrent connections much better than Turso's Hrana protocol,
making it ideal when using `channel_workers > 1`.

**Free PostgreSQL hosting options:**
- [Neon](https://neon.tech) - Serverless Postgres, generous free tier
- [Supabase](https://supabase.com) - Postgres with extras, 500MB free

## Parallel Processing

The fetcher supports parallel processing at two levels for faster runs:

### Channel Workers
Process multiple channels simultaneously. Set via config or CLI:

```yaml
# config/channels.yaml
settings:
  channel_workers: 2  # Process 2 channels in parallel
```

```bash
python fetch.py --channel-workers 2
```

Each worker gets its own database connection and YouTube API client to avoid conflicts.

### Comment Workers
Fetch comments for multiple videos in parallel within a channel:

```yaml
settings:
  comment_workers: 3  # Fetch comments from 3 videos simultaneously
```

### Thread-Safety Design

**With PostgreSQL (recommended for parallel):**
PostgreSQL handles concurrent connections natively. Full parallel processing supported.

**With Turso/libsql:**
Turso's native library (`libsql-experimental`) has thread-safety issues at the C memory allocator level,
causing heap corruption when used with multiple threads. **Parallel channel workers are automatically
disabled** when using Turso. Comment workers within a single channel still work since they share one connection.

Turso-specific features:
- **Connection refresh**: Automatic reconnection on "stream not found" errors
- **Auto-checkpointing**: Quota state saves every 500 units + at phase transitions

## API Quota Management

YouTube Data API v3 quota: **10,000 units/day** (default)

| Operation | Cost |
|-----------|------|
| channels.list | 1 |
| playlists.list | 1 |
| playlistItems.list | 1 |
| videos.list | 1 |
| commentThreads.list | 1 |
| search.list | 100 |

**Typical usage per channel:**
- Basic fetch (100 videos): ~10 units
- With comments: ~100-500 units
- Transcripts: 0 (doesn't use YouTube API)

## Configuration

### channels.yaml

```yaml
channels:
  # Simple format
  - "@GoogleDevelopers"
  - "@Android"
  
  # Detailed format with per-channel options
  - identifier: "@TheMajorityReport"
    max_videos: 1000
    max_video_age_days: 90
    fetch_comments: false
    comments_update_hours: 12
    comments_update_hours_new: 2

settings:
  comments_update_hours: 24      # Default for older videos
  comments_update_hours_new: 6   # Default for new videos
  new_video_days: 7              # Videos < 7 days old are "new"
```

## Troubleshooting

### "Quota exhausted"
- Quota is tracked in the database and resets at midnight
- Use `--skip-comments` to reduce usage
- Check usage with `python analyse.py --report summary`

### "All transcripts unavailable"
- Transcripts must be fetched locally, not from CI/cloud
- Run `python fetch_transcripts.py` from your local machine
- Use `--dry-run` to test before saving

### "YOUTUBE_API_KEY not provided"
- Ensure environment variable is set
- Check GitHub Actions secrets

### "Could not resolve channel ID"
- Verify the channel exists
- Try using the full channel ID (UC...)

### Resuming failed runs
- Progress is automatically saved
- Just run the same command again
- Check logs for what was completed

## License

MIT License
