# YouTube Channel Analytics Tracker

A production-ready system for tracking YouTube channel metadata over time. Designed for ongoing analysis of multiple channels with efficient incremental updates.

## Features

- **Multi-channel tracking** - Monitor dozens of channels from a single config file
- **Incremental updates** - Only fetches new data since last run
- **Time-series storage** - Track view counts, subscribers, comments over time
- **DuckDB backend** - Fast SQL queries, single-file database, Parquet export
- **Smart caching** - Transcripts fetched once, comments deduplicated
- **Rate limiting** - Built-in API quota management
- **GitHub Actions** - Scheduled 4x daily fetches with artifact storage

## Data Collected

| Data Type | Update Frequency | Storage |
|-----------|-----------------|---------|
| Channel metadata | Every run | Upserted |
| Channel stats (subscribers, views) | Every run | Append-only time series |
| Video metadata | Every run | Upserted |
| Video stats (views, likes) | Every run | Append-only time series |
| Chapters | When video changes | Replaced |
| Transcripts | Once per video | Write-once |
| Comments | Incremental (new only) | Append with dedup |
| Playlists | Every run | Upserted |

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up API Key

```bash
export YOUTUBE_API_KEY="your-api-key-here"
```

Get an API key from [Google Cloud Console](https://console.cloud.google.com/):
1. Create/select a project
2. Enable "YouTube Data API v3"
3. Create credentials → API Key

### 3. Create Channel Config

```bash
cp config/channels.example.yaml config/channels.yaml
# Edit config/channels.yaml with your channels
```

### 4. Run Initial Fetch

```bash
cd scripts

# Single channel
python fetch.py --channel @GoogleDevelopers --backfill

# Multiple channels from config
python fetch.py --config ../config/channels.yaml --backfill
```

### 5. Run Analysis

```bash
# Database summary
python analyze.py --report summary

# Top growing videos
python analyze.py --report growth

# Custom SQL
python analyze.py --sql "SELECT title, view_count FROM videos ORDER BY view_count DESC LIMIT 10"

# Export to CSV
python analyze.py --report top-videos --output top_videos.csv
```

## Project Structure

```
├── scripts/
│   ├── fetch.py          # Main fetcher script
│   ├── analyze.py        # Query and reporting tool
│   ├── database.py       # DuckDB schema and operations
│   └── youtube_api.py    # YouTube API client
├── config/
│   └── channels.yaml     # Channels to track
├── data/
│   └── youtube.duckdb    # Database (created on first run)
├── exports/              # Parquet exports
└── .github/workflows/
    └── fetch.yml         # GitHub Actions workflow
```

## GitHub Actions Setup

### 1. Add Repository Secrets

Go to Settings → Secrets → Actions:
- `YOUTUBE_API_KEY`: Your YouTube Data API key

### 2. Create Config File

Either:
- Commit `config/channels.yaml` to your repo, or
- The workflow will use `channels.example.yaml` as fallback

### 3. Enable Workflow

The workflow runs automatically:
- **4x daily**: Midnight, 6 AM, Noon, 6 PM UTC
- **Manual**: Via Actions tab → Run workflow
- **Weekly export**: Sundays at midnight (Parquet files)

### 4. Access Data

- **Database**: Download from Actions → workflow run → Artifacts
- **Parquet exports**: Weekly exports for external analysis

## CLI Reference

### fetch.py

```bash
# Single channel (backfill = full fetch)
python fetch.py --channel @GoogleDevelopers --backfill

# From config (incremental)
python fetch.py --config ../config/channels.yaml

# Skip expensive operations
python fetch.py --config ../config/channels.yaml --skip-comments --skip-transcripts

# Limit videos (useful for huge channels)
python fetch.py --channel @YouTube --max-videos 1000

# Export database to Parquet
python fetch.py --export
```

### analyze.py

```bash
# List available reports
python analyze.py --list-reports

# Run preset report
python analyze.py --report growth
python analyze.py --report top-videos
python analyze.py --report comment-velocity
python analyze.py --report engagement-rate
python analyze.py --report subscriber-growth

# Custom SQL
python analyze.py --sql "SELECT * FROM channels"

# Export to CSV
python analyze.py --report growth --output growth.csv
```

## Available Reports

| Report | Description |
|--------|-------------|
| `summary` | Overall database statistics |
| `channels` | All tracked channels with latest stats |
| `growth` | Video view growth over last 7 days |
| `top-videos` | Highest view count videos |
| `recent-uploads` | Most recent uploads across channels |
| `comment-velocity` | Videos with most comment activity |
| `engagement-rate` | Likes+comments per view |
| `subscriber-growth` | Daily subscriber changes |
| `transcript-coverage` | Transcript availability by channel |
| `popular-commenters` | Most active commenters |
| `video-length-performance` | Performance by duration |
| `fetch-history` | Recent fetch operations |

## Database Schema

### Core Tables

```sql
-- Channel info and stats
channels (channel_id, title, description, ...)
channel_stats (channel_id, fetched_at, subscriber_count, view_count, video_count)

-- Video info and stats
videos (video_id, channel_id, title, published_at, duration_seconds, ...)
video_stats (video_id, fetched_at, view_count, like_count, comment_count)

-- Content
chapters (video_id, chapter_index, title, start_seconds, end_seconds)
transcripts (video_id, language, full_text, entries_json)
comments (comment_id, video_id, parent_comment_id, author, text, published_at, ...)
playlists (playlist_id, channel_id, title, item_count, ...)

-- Metadata
fetch_log (fetch_id, channel_id, fetch_type, started_at, completed_at, status, ...)
```

### Example Queries

```sql
-- View growth over time for a specific video
SELECT fetched_at, view_count 
FROM video_stats 
WHERE video_id = 'dQw4w9WgXcQ'
ORDER BY fetched_at;

-- Compare channels
SELECT c.title, 
       MAX(cs.subscriber_count) as subs,
       COUNT(DISTINCT v.video_id) as videos
FROM channels c
JOIN channel_stats cs ON c.channel_id = cs.channel_id
JOIN videos v ON c.channel_id = v.channel_id
GROUP BY c.channel_id, c.title;

-- Find videos about specific topic (using transcripts)
SELECT v.title, v.video_id
FROM videos v
JOIN transcripts t ON v.video_id = t.video_id
WHERE t.full_text ILIKE '%machine learning%';
```

## API Quota Management

YouTube Data API quota: **10,000 units/day** (default)

| Operation | Cost | Notes |
|-----------|------|-------|
| Channel list | 1 | Per request |
| Playlist items | 1 | Per page (50 items) |
| Videos list | 1 | Per page (50 videos) |
| Comment threads | 1 | Per page (100 comments) |

**Estimated usage per channel:**
- Basic metadata: ~50 units
- With comments (500/video): ~500-2000 units
- Transcripts: 0 (doesn't use API)

**For 20 channels, 4x daily:**
- Without comments: ~4,000 units/day ✅
- With comments: May exceed quota ⚠️

Tips:
- Use `--skip-comments` for some runs
- Limit `--max-comments` per video
- Transcripts are free (scraped, not API)

## Scaling Considerations

### For 90 days × dozens of channels

- **Database size**: ~100MB-1GB depending on comment volume
- **Parquet export**: More efficient for heavy analysis
- **Query performance**: DuckDB handles millions of rows easily
- **GitHub artifacts**: 90-day retention, may need external storage for longer

### External Storage Options

For long-term storage beyond GitHub artifacts:
- Export Parquet to S3/GCS
- Use GitHub releases for snapshots
- Sync to external database

## Comment Fields

Each comment includes:

| Field | Description |
|-------|-------------|
| `comment_id` | Unique identifier |
| `video_id` | Parent video |
| `parent_comment_id` | NULL for top-level, ID for replies |
| `author_display_name` | Commenter's display name |
| `author_channel_id` | Commenter's channel ID |
| `text` | Comment text (plain text) |
| `like_count` | Number of likes |
| `published_at` | Original post time |
| `updated_at` | Last edit time |
| `fetched_at` | When we retrieved it |

## License

MIT License