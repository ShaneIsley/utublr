# YouTube Channel Analytics Tracker

A production-ready system for tracking YouTube channel metadata over time using Turso (cloud SQLite) for persistent storage.

## Features

- **Multi-channel tracking** - Monitor dozens of channels from a single config file
- **Smart incremental updates** - Only fetches new/changed data to minimize API usage
- **Turso cloud database** - No more artifact uploads; data persists reliably
- **Quota tracking** - Monitors API usage, warns before exhaustion
- **Checkpointing** - Resume interrupted fetches from where they left off
- **Retry with backoff** - Handles transient API errors gracefully
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
```

## CLI Reference

### fetch.py

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
    --max-replies 10 \
    --batch-size 10

# Skip expensive operations
python fetch.py --config ../config/channels.yaml \
    --skip-comments \
    --skip-transcripts

# Control update frequency
python fetch.py --config ../config/channels.yaml \
    --stats-update-hours 6 \
    --comments-update-hours 24

# Export to CSV
python fetch.py --export
```

### Command Line Options

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
| `--max-replies` | 10 | Max replies per comment (prevents blowup) |
| `--batch-size` | 10 | Videos per batch/commit |
| `--stats-update-hours` | 6 | Skip stats if updated within N hours |
| `--comments-update-hours` | 24 | Skip comments if fetched within N hours |
| `--max-runtime-minutes` | 300 | Stop after N minutes |
| `--quota-limit` | 10000 | Daily API quota limit |
| `--skip-comments` | false | Don't fetch comments |
| `--skip-transcripts` | false | Don't fetch transcripts |

### analyze.py

```bash
# List available reports
python analyze.py --list-reports

# Run preset reports
python analyze.py --report summary
python analyze.py --report channels
python analyze.py --report growth
python analyze.py --report top-videos

# Custom SQL
python analyze.py --sql "SELECT title, view_count FROM videos ORDER BY view_count DESC LIMIT 10"

# Export to CSV
python analyze.py --report growth --output growth.csv
```

## Smart Incremental Fetching

The system minimizes redundant API requests:

| Data Type | Update Strategy | Skip If... |
|-----------|-----------------|------------|
| Channel stats | Every 6 hours | Updated within `--stats-update-hours` |
| Video metadata | Only new videos | Video already in DB |
| Video stats | Stale videos only | Updated within `--stats-update-hours` |
| Transcripts | Once per video | Already have transcript |
| Comments | Stale videos only | Fetched within `--comments-update-hours` |

## Error Handling

### Retry Logic
All API calls retry up to 3 times with exponential backoff for:
- HTTP 429 (rate limit)
- HTTP 500/502/503/504 (server errors)
- Connection errors

### Checkpointing
Progress is saved after each batch. If a run fails:
1. Videos already processed are saved
2. Next run resumes from where it left off
3. No duplicate work or data loss

### Quota Protection
- Tracks quota usage across runs (persisted to file)
- Warns at 80% usage
- Aborts at 95% usage
- Estimates cost before fetching each channel

## Logging

Logs are written to `scripts/logs/` with DEBUG level:

```
logs/
├── fetch_20250103_143052.log  # Timestamped log files
└── latest.log                  # Symlink to most recent
```

Log format:
```
2025-01-03 14:30:52 | DEBUG    | youtube_api:fetch_channel:245 | Fetching channel metadata for: UCxyz...
```

Set log levels via environment:
```bash
export LOG_LEVEL=DEBUG          # File log level (default: DEBUG)
export CONSOLE_LOG_LEVEL=INFO   # Console log level (default: INFO)
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
```

### Deleted Video Handling

When a video is no longer in a channel's uploads:
1. Marked as `privacy_status = 'deleted'`
2. Stats/comments preserved for analysis
3. Can be purged after N days if desired

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
- Transcripts: 0 (doesn't use API)

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
    fetch_transcripts: true
```

## Troubleshooting

### "Quota exhausted"
- Check `data/quota_state.json` for current usage
- Wait until midnight Pacific time for reset
- Use `--skip-comments` to reduce usage

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
