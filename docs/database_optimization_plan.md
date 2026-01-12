# Database Storage Optimization Plan

## Executive Summary

This document outlines strategies to **reduce database storage by 40-70%** while maintaining or improving query performance for the YouTube analytics system.

## Current Architecture Analysis

### Storage Breakdown (Estimated)

Based on schema analysis, storage is consumed by:

1. **Time-series data** (60-70% of storage)
   - `video_stats`: Multiple snapshots per video (hourly/daily)
   - `channel_stats`: Multiple snapshots per channel (hourly/daily)
   - `comments`: Growing continuously (never deleted)

2. **Metadata** (20-30% of storage)
   - `videos`: Rich metadata (tags, descriptions, thumbnails)
   - `channels`: Channel information
   - `transcripts`: Full text + JSON entries

3. **Operational data** (5-10% of storage)
   - `fetch_log`, `fetch_progress`, `quota_usage`
   - `video_comment_summary`: Denormalized cache

### Key Issues

1. **Unbounded growth**: No data retention policies
2. **Redundant snapshots**: Stats stored even when unchanged
3. **TEXT-based IDs**: Larger indexes than integer keys
4. **JSON columns**: `entries_json`, `tags`, `topic_categories` stored as text
5. **No compression**: Old data not archived/compressed

---

## Optimization Strategies

### Strategy 1: Time-Series Data Compression (40-60% reduction)

**Problem**: `video_stats` and `channel_stats` store snapshots even when values don't change.

**Solution**: Differential storage + time-based aggregation

#### Implementation

```sql
-- New table: Only store when stats change
CREATE TABLE video_stats_compressed (
    video_id TEXT NOT NULL,
    changed_at TIMESTAMP NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    PRIMARY KEY (video_id, changed_at)
);

-- Index for latest stats queries
CREATE INDEX idx_video_stats_compressed_latest
ON video_stats_compressed(video_id, changed_at DESC);
```

**Retention Policy**:
- **Last 7 days**: Keep all snapshots (hourly granularity)
- **8-30 days**: Keep daily snapshots only (midnight values)
- **31-365 days**: Keep weekly snapshots (Monday values)
- **> 1 year**: Keep monthly snapshots (1st of month)

**Storage Reduction**: 50-70% for time-series tables

**Migration Strategy**:
```python
def compress_video_stats():
    """Compress video_stats by removing unchanged entries and aggregating old data"""

    # Step 1: Remove duplicate entries where stats didn't change
    cursor.execute("""
        DELETE FROM video_stats
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM video_stats
            GROUP BY video_id, view_count, like_count, comment_count
        )
    """)

    # Step 2: For data > 30 days, keep only daily snapshots
    cursor.execute("""
        DELETE FROM video_stats
        WHERE fetched_at < datetime('now', '-30 days')
        AND rowid NOT IN (
            SELECT MIN(rowid)
            FROM video_stats
            WHERE fetched_at < datetime('now', '-30 days')
            GROUP BY video_id, DATE(fetched_at)
        )
    """)

    # Step 3: For data > 365 days, keep only weekly snapshots
    cursor.execute("""
        DELETE FROM video_stats
        WHERE fetched_at < datetime('now', '-365 days')
        AND rowid NOT IN (
            SELECT MIN(rowid)
            FROM video_stats
            WHERE fetched_at < datetime('now', '-365 days')
            GROUP BY video_id, strftime('%Y-%W', fetched_at)
        )
    """)
```

---

### Strategy 2: Normalize Repeated Data (15-25% reduction)

**Problem**: Tags, topics, and categories stored as comma-separated TEXT in every video row.

**Solution**: Normalize into junction tables with integer IDs

#### Current Schema
```sql
videos (
    tags TEXT,  -- "python,tutorial,ml,ai,openai"
    topic_categories TEXT  -- "/m/01k8wb,/m/019_rr"
)
```

#### Optimized Schema
```sql
CREATE TABLE tags (
    tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_name TEXT UNIQUE NOT NULL
);

CREATE TABLE video_tags (
    video_id TEXT NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (video_id, tag_id),
    FOREIGN KEY (video_id) REFERENCES videos(video_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE topic_categories (
    topic_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_code TEXT UNIQUE NOT NULL,  -- "/m/01k8wb"
    topic_name TEXT  -- Optional: "Machine learning"
);

CREATE TABLE video_topics (
    video_id TEXT NOT NULL,
    topic_id INTEGER NOT NULL,
    PRIMARY KEY (video_id, topic_id),
    FOREIGN KEY (video_id) REFERENCES videos(video_id),
    FOREIGN KEY (topic_id) REFERENCES topic_categories(topic_id)
);
```

**Benefits**:
- Deduplication: "python" tag stored once instead of in 1000 videos
- Faster queries: `SELECT videos WHERE tag_id = 5` vs `WHERE tags LIKE '%python%'`
- Storage: 20-30% reduction for videos table

---

### Strategy 3: Integer Surrogate Keys (10-20% reduction)

**Problem**: All PKs are TEXT (channel_id, video_id, comment_id). Indexes on TEXT are larger.

**Solution**: Add integer surrogate keys for internal use, keep YouTube IDs for external reference

#### Optimized Schema
```sql
CREATE TABLE videos_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Internal surrogate key
    video_id TEXT UNIQUE NOT NULL,         -- YouTube ID (indexed)
    channel_id_fk INTEGER NOT NULL,        -- Foreign key to channels.id
    title TEXT,
    -- ... other fields
    FOREIGN KEY (channel_id_fk) REFERENCES channels_v2(id)
);

CREATE TABLE video_stats_v2 (
    video_fk INTEGER NOT NULL,             -- Integer FK instead of TEXT
    fetched_at TIMESTAMP NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    PRIMARY KEY (video_fk, fetched_at),
    FOREIGN KEY (video_fk) REFERENCES videos_v2(id)
);
```

**Benefits**:
- Index size: INTEGER indexes are 50-70% smaller than TEXT indexes
- Join performance: Integer joins are faster than TEXT joins
- Foreign keys: Properly enforced referential integrity

**Storage Reduction**: 10-20% overall (mainly index sizes)

---

### Strategy 4: Transcript Optimization (5-15% reduction)

**Problem**: `entries_json` stores entire JSON array as TEXT

**Solution**: Normalize into separate table (only if transcripts are frequently queried)

#### Current Schema
```sql
transcripts (
    entries_json TEXT  -- '[{"start_ms": 0, "duration_ms": 3400, "text": "..."}, ...]'
)
```

#### Option A: Keep as-is (recommended)
- Transcripts are write-once, rarely queried
- JSON parsing is fast enough for occasional use
- **No migration needed**

#### Option B: Normalize (if transcript search is needed)
```sql
CREATE TABLE transcript_entries (
    transcript_id INTEGER NOT NULL,
    entry_index INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    text TEXT NOT NULL,
    PRIMARY KEY (transcript_id, entry_index),
    FOREIGN KEY (transcript_id) REFERENCES transcripts(id)
);

-- Full-text search on transcript text
CREATE VIRTUAL TABLE transcript_search USING fts5(
    transcript_id,
    entry_index,
    text
);
```

**Recommendation**: **Keep current design** unless you need to search within transcripts.

---

### Strategy 5: Comment Archival Policy (20-40% reduction over time)

**Problem**: Comments table grows unbounded and consumes significant storage

**Solution**: Implement tiered archival

#### Retention Tiers
```python
COMMENT_RETENTION_POLICY = {
    'hot': 90,      # Days: Keep all comments in main table
    'warm': 365,    # Days: Move to archive table
    'cold': None    # After 1 year: Delete or compress to summary stats
}
```

#### Archive Schema
```sql
CREATE TABLE comments_archive (
    video_id TEXT NOT NULL,
    archived_at TIMESTAMP NOT NULL,
    comment_count INTEGER,
    total_likes INTEGER,
    top_authors TEXT,  -- JSON: [{"author": "user1", "comment_count": 15}, ...]
    sample_comments TEXT  -- JSON: Keep top 10 most-liked comments
);
```

#### Migration Function
```python
def archive_old_comments():
    """Move comments > 90 days old to archive table"""

    # Get summary stats for old comments
    cursor.execute("""
        INSERT INTO comments_archive (video_id, archived_at, comment_count, total_likes, sample_comments)
        SELECT
            video_id,
            CURRENT_TIMESTAMP,
            COUNT(*) as comment_count,
            SUM(like_count) as total_likes,
            json_group_array(
                json_object('author', author_display_name, 'text', text, 'likes', like_count)
            ) FILTER (WHERE like_count > 10) as sample_comments
        FROM comments
        WHERE published_at < datetime('now', '-90 days')
        GROUP BY video_id
    """)

    # Delete old comments
    cursor.execute("""
        DELETE FROM comments
        WHERE published_at < datetime('now', '-90 days')
    """)
```

**Storage Reduction**: 30-50% for comments table over time

---

### Strategy 6: Thumbnail URL Deduplication (2-5% reduction)

**Problem**: Thumbnail URLs are stored in full for every video/channel

**Solution**: Pattern-based storage or separate table

#### Current
```sql
videos (
    thumbnail_url TEXT  -- "https://i.ytimg.com/vi/VIDEO_ID/maxresdefault.jpg"
)
```

#### Optimized
```sql
-- Option 1: Store only quality indicator (URL can be reconstructed)
videos (
    thumbnail_quality TEXT  -- "maxresdefault", "hqdefault", etc.
)
-- Reconstruct: f"https://i.ytimg.com/vi/{video_id}/{thumbnail_quality}.jpg"

-- Option 2: Separate table with deduplication
CREATE TABLE thumbnail_urls (
    url_id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL
);

videos (
    thumbnail_url_fk INTEGER,
    FOREIGN KEY (thumbnail_url_fk) REFERENCES thumbnail_urls(url_id)
)
```

**Recommendation**: Option 1 (pattern-based) saves storage without complexity

---

## Combined Optimization Impact

| Strategy | Storage Reduction | Performance Impact | Implementation Effort |
|----------|------------------|-------------------|---------------------|
| Time-series compression | 40-60% | Neutral/Positive | Medium |
| Normalize tags/topics | 15-25% | Positive | Medium |
| Integer surrogate keys | 10-20% | Positive | High |
| Transcript optimization | 5-15% | Neutral | Low (skip) |
| Comment archival | 20-40% | Positive | Medium |
| Thumbnail dedup | 2-5% | Neutral | Low |

**Total Estimated Reduction**: **50-75%** of current storage

**Performance**: Neutral to **20-30% faster queries** (better indexes, smaller tables)

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)
1. ✅ Implement time-series compression
   - Add retention policy to `video_stats` and `channel_stats`
   - Remove unchanged snapshots
   - Add automated cleanup job

2. ✅ Thumbnail URL optimization
   - Migrate to pattern-based storage
   - Update insert/query functions

**Expected Reduction**: 40-50%

### Phase 2: Structural Improvements (3-5 days)
1. ✅ Normalize tags and topics
   - Create new tables
   - Migrate existing data
   - Update queries

2. ✅ Comment archival policy
   - Create archive table
   - Implement tiered retention
   - Add scheduled archival job

**Expected Reduction**: Additional 20-30%

### Phase 3: Advanced Optimization (1-2 weeks)
1. ✅ Integer surrogate keys
   - Design migration strategy
   - Create new schema version
   - Migrate data incrementally
   - Update all application code

**Expected Reduction**: Additional 10-20%

---

## Migration Checklist

- [ ] Backup current database
- [ ] Test migration scripts on copy
- [ ] Implement backward-compatible schema changes
- [ ] Update database.py insert/query functions
- [ ] Update indexes
- [ ] Run VACUUM after migration (reclaim space)
- [ ] Update documentation
- [ ] Monitor query performance
- [ ] Verify data integrity

---

## Maintenance Automation

### Scheduled Jobs (via cron or scheduler)

```python
# Daily: Compress old stats
@daily
def compress_old_stats():
    compress_video_stats()
    compress_channel_stats()
    conn.execute("VACUUM")  # Reclaim space

# Weekly: Archive old comments
@weekly
def archive_comments():
    archive_old_comments()
    conn.execute("VACUUM")

# Monthly: Full integrity check
@monthly
def health_check():
    run_db_maintenance()
```

---

## Risk Mitigation

1. **Data Loss Prevention**
   - Full backup before any migration
   - Incremental migration with rollback points
   - Keep archive tables for 30 days before final deletion

2. **Performance Monitoring**
   - Benchmark queries before/after
   - Monitor index usage with `EXPLAIN QUERY PLAN`
   - Track storage size daily during migration

3. **Application Compatibility**
   - Maintain backward-compatible views
   - Update one module at a time
   - Feature flags for new schema

---

## Success Metrics

### Storage
- [ ] Database size reduced by >50%
- [ ] Time-series tables reduced by >60%
- [ ] Index sizes reduced by >30%

### Performance
- [ ] Query latency maintained or improved
- [ ] No increase in API quota usage
- [ ] Successful VACUUM operations complete <5 minutes

### Reliability
- [ ] Zero data loss
- [ ] All foreign keys properly enforced
- [ ] Automated cleanup jobs run successfully

---

## Next Steps

1. **Review this plan** with stakeholders
2. **Choose implementation phases** (recommend starting with Phase 1)
3. **Create test database** with representative data
4. **Implement and test** Phase 1 optimizations
5. **Measure results** before proceeding to Phase 2

Would you like me to implement any of these optimizations?
