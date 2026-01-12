# Database Optimization Guide

## Quick Start

This guide provides practical instructions for reducing your database storage by 40-70% while maintaining or improving query performance.

## Overview

Your YouTube analytics database can be optimized in three phases:

- **Phase 1** (Quick Wins): Time-series compression + thumbnail optimization → **40-50% reduction**
- **Phase 2** (Structural): Normalize tags/topics + comment archival → **Additional 20-30% reduction**
- **Phase 3** (Advanced): Integer surrogate keys → **Additional 10-20% reduction**

## Phase 1: Quick Wins (Recommended Start)

### 1. Run Dry-Run Analysis

First, see what would be optimized without making changes:

```bash
python scripts/optimize_database.py --dry-run --all
```

This shows:
- How many duplicate stats entries exist
- How much old time-series data can be aggregated
- How many thumbnails can be optimized
- Estimated space savings

### 2. Run Full Optimization

When ready, run the actual optimization:

```bash
python scripts/optimize_database.py --all
```

This will:
1. Remove duplicate stats entries (where values didn't change)
2. Aggregate old time-series data:
   - Last 30 days: Keep all snapshots (hourly)
   - 30-90 days: Keep daily snapshots only
   - 90-365 days: Keep weekly snapshots only
   - > 365 days: Keep monthly snapshots only
3. Optimize thumbnail URLs (extract quality indicators)
4. Run VACUUM to reclaim disk space

**Expected Results**: 40-60% storage reduction

### 3. Individual Optimizations

You can also run optimizations separately:

```bash
# Only compress time-series stats
python scripts/optimize_database.py --compress-stats

# Only optimize thumbnails
python scripts/optimize_database.py --optimize-thumbs
```

## Automated Maintenance

### Set Up Scheduled Jobs

To keep your database optimized, set up automated maintenance tasks:

#### Option 1: Crontab (Linux/Mac)

Edit your crontab:
```bash
crontab -e
```

Add these entries:
```bash
# Daily at 2 AM: Compress old stats
0 2 * * * cd /home/user/utublr && python scripts/scheduled_optimization.py --daily >> logs/optimization.log 2>&1

# Weekly on Sunday at 3 AM: Archive comments and full compression
0 3 * * 0 cd /home/user/utublr && python scripts/scheduled_optimization.py --weekly >> logs/optimization.log 2>&1

# Monthly on 1st at 4 AM: Full maintenance with health checks
0 4 1 * * cd /home/user/utublr && python scripts/scheduled_optimization.py --monthly >> logs/optimization.log 2>&1
```

#### Option 2: Systemd Timers (Linux)

Create timer files in `/etc/systemd/system/`:

**daily-db-optimization.timer**:
```ini
[Unit]
Description=Daily database optimization

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

**daily-db-optimization.service**:
```ini
[Unit]
Description=Daily database optimization

[Service]
Type=oneshot
WorkingDirectory=/home/user/utublr
ExecStart=/usr/bin/python3 scripts/scheduled_optimization.py --daily
```

Enable and start:
```bash
sudo systemctl enable --now daily-db-optimization.timer
```

#### Option 3: Manual Runs

Run maintenance tasks manually as needed:

```bash
# Daily maintenance (compress recent stats)
python scripts/scheduled_optimization.py --daily

# Weekly maintenance (compress stats + archive comments)
python scripts/scheduled_optimization.py --weekly

# Monthly maintenance (full optimization + health checks)
python scripts/scheduled_optimization.py --monthly
```

## Monitoring

### Check Optimization Logs

```bash
tail -f logs/optimization.log
```

### Database Size Tracking

Check current database size:

```bash
# SQLite
du -sh data/youtube.db

# Or via SQL
sqlite3 data/youtube.db "SELECT page_count * page_size / 1024.0 / 1024.0 as size_mb FROM pragma_page_count(), pragma_page_size()"
```

### Table Size Analysis

See which tables consume the most space:

```bash
sqlite3 data/youtube.db "
SELECT
    name as table_name,
    ROUND(SUM(pgsize)/1024.0/1024.0, 2) as size_mb
FROM dbstat
WHERE name NOT LIKE 'sqlite_%'
GROUP BY name
ORDER BY size_mb DESC
LIMIT 10;
"
```

## Phase 2: Structural Improvements (Optional)

For even more optimization, consider implementing Phase 2 improvements:

### Normalize Tags and Topics

Benefits:
- Deduplicates repeated tag/topic names
- Enables faster tag-based queries
- Reduces video table size by 20-30%

**Implementation**: See `/docs/database_optimization_plan.md` section "Strategy 2"

### Comment Archival

Benefits:
- Reduces active table size
- Keeps summary statistics
- Maintains top comments for reference

**Already implemented** in `scheduled_optimization.py --weekly`

## Phase 3: Advanced Optimization (Future)

For maximum optimization:

### Integer Surrogate Keys

Benefits:
- 50-70% smaller indexes
- Faster joins
- Proper foreign key constraints

**Trade-offs**:
- Requires significant schema migration
- All queries need updates
- Higher implementation effort

See `/docs/database_optimization_plan.md` section "Strategy 3" for details.

## Troubleshooting

### VACUUM Takes Too Long

If VACUUM runs for more than 10 minutes:

1. Check available disk space (needs 2x database size temporarily)
2. Run incremental VACUUMs:
   ```bash
   sqlite3 data/youtube.db "PRAGMA incremental_vacuum(1000)"
   ```

### Script Errors

Check logs for details:
```bash
cat logs/optimization.log | grep ERROR
```

Common issues:
- Database locked: Close other connections
- Disk full: Free up space before VACUUM
- Import errors: Ensure you're in the project directory

### Verify Data Integrity

After optimization, run integrity checks:

```bash
sqlite3 data/youtube.db "PRAGMA integrity_check"
```

## Best Practices

1. **Backup before optimizing**:
   ```bash
   cp data/youtube.db data/youtube.db.backup
   ```

2. **Test on a copy first**:
   ```bash
   cp data/youtube.db data/youtube_test.db
   # Run optimizations on test database
   # Verify results before applying to production
   ```

3. **Monitor performance**: Track query times before/after optimization

4. **Run incrementally**: Start with Phase 1, measure results, then proceed

5. **Schedule maintenance**: Set up automated jobs to maintain optimization

## Expected Results

After implementing Phase 1 optimizations:

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Database size | 1000 MB | 500-600 MB | 40-50% |
| video_stats rows | 100,000 | 40,000 | 60% |
| channel_stats rows | 5,000 | 2,000 | 60% |
| Query performance | Baseline | Same or better | 0-20% faster |

## Support

For detailed technical information, see:
- `/docs/database_optimization_plan.md` - Full technical specification
- `/docs/performance_analysis.md` - Performance benchmarks
- `/scripts/db_maintenance.py` - Database health checks

## Next Steps

1. ✅ Run dry-run analysis: `python scripts/optimize_database.py --dry-run --all`
2. ✅ Review output and confirm optimizations
3. ✅ Backup database: `cp data/youtube.db data/youtube.db.backup`
4. ✅ Run Phase 1: `python scripts/optimize_database.py --all`
5. ✅ Set up scheduled maintenance
6. ✅ Monitor results for 1 week
7. Consider Phase 2 optimizations if needed
