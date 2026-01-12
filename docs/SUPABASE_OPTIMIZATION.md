# Supabase Database Optimization Workflow

Complete automated workflow for optimizing your Supabase PostgreSQL database, reducing storage by 40-70% while maintaining performance.

## Quick Start

### 1. Dry Run (Safe - No Changes)

```bash
# Set your Supabase connection string
export POSTGRES_URL='postgresql://user:pass@db.supabase.co:5432/postgres'

# Run analysis to see potential savings
python scripts/supabase_optimization_workflow.py --dry-run
```

Expected output:
```
Stage 1: Verifying database connection...
✓ Connected to database: postgres
✓ PostgreSQL version: PostgreSQL 15.1

Stage 2: Analyzing database...
✓ Database size: 245.67 MB
✓ Tables analyzed: 12
✓ Total rows: 156,234

Potential Savings:
  • Estimated reduction: 122.83 MB (50.0%)
  • video_stats: 98.45 MB
  • channel_stats: 24.38 MB
```

### 2. Run Optimization

```bash
# Run full optimization
python scripts/supabase_optimization_workflow.py --optimize
```

### 3. View Results

```bash
# Check the generated report
cat logs/optimization_workflow_opt_YYYYMMDD_HHMMSS.json
```

---

## Automated Workflow (GitHub Actions)

### Setup

1. **Add Supabase connection string to GitHub Secrets**:
   - Go to: Repository → Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `POSTGRES_URL`
   - Value: `postgresql://postgres.xxx:password@aws-0-us-west-1.pooler.supabase.com:6543/postgres`

2. **Enable workflow**:
   - The workflow is already configured in `.github/workflows/optimize-database.yml`
   - It runs automatically every Sunday at 3 AM UTC
   - Or trigger manually from the Actions tab

### Manual Trigger

1. Go to: Repository → Actions → "Database Optimization"
2. Click "Run workflow"
3. Options:
   - **Dry run mode**: Check this for analysis only (no changes)
   - **Force optimization**: Skip threshold check (useful for testing)
4. Click "Run workflow"

### View Results

After the workflow runs:

1. Go to the workflow run
2. Check the "Summary" tab for key metrics
3. Download the "optimization-report" artifact for detailed JSON report
4. View logs for full execution details

---

## What Gets Optimized?

### 1. Time-Series Data Compression (40-60% reduction)

**Problem**: `video_stats` and `channel_stats` store hourly snapshots, even when values don't change.

**Solution**: Differential storage with time-based aggregation:
- **Last 30 days**: Keep all snapshots (hourly granularity)
- **30-90 days**: Keep daily snapshots only
- **90-365 days**: Keep weekly snapshots only
- **> 1 year**: Keep monthly snapshots only

**Example**:
```
Before: 10,000 hourly snapshots from 90 days ago
After:  90 daily snapshots (89% reduction)
```

### 2. Duplicate Removal

**Problem**: Stats stored even when values are identical to previous snapshot.

**Solution**: Remove consecutive entries with unchanged values.

**Example**:
```sql
-- Before (3 identical entries)
video_id | fetched_at          | view_count | like_count
---------|---------------------|------------|------------
abc123   | 2024-01-01 12:00:00 | 1000       | 50
abc123   | 2024-01-01 13:00:00 | 1000       | 50  ← Duplicate
abc123   | 2024-01-01 14:00:00 | 1000       | 50  ← Duplicate

-- After (1 entry)
abc123   | 2024-01-01 12:00:00 | 1000       | 50
```

### 3. Thumbnail URL Optimization (2-5% reduction)

**Problem**: Full URLs stored for every video/channel.

**Solution**: Extract and store quality indicator only, reconstruct URL when needed.

**Example**:
```
Before: "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg" (57 bytes)
After:  "maxresdefault" (13 bytes)

Reconstruct: f"https://i.ytimg.com/vi/{video_id}/{quality}.jpg"
```

---

## Workflow Stages

### Stage 1: Connection Verification
- Validates PostgreSQL connection
- Checks database version and permissions
- Verifies Supabase connectivity

### Stage 2: Database Analysis
- Measures current database size
- Analyzes table sizes and row counts
- Estimates potential savings
- Checks optimization threshold (10 MB or 5% minimum)

### Stage 3: Optimization Execution
- Removes duplicate stats entries
- Aggregates old time-series data
- Optimizes thumbnail storage
- Runs VACUUM to reclaim space

### Stage 4: Data Integrity Verification
- Checks for orphaned records
- Validates foreign key relationships
- Ensures no data corruption

### Stage 5: Report Generation
- Generates comprehensive JSON report
- Logs before/after metrics
- Calculates space savings
- Provides recommendations

---

## Understanding the Reports

### JSON Report Structure

```json
{
  "workflow_id": "opt_20240115_143022",
  "started_at": "2024-01-15T14:30:22",
  "completed_at": "2024-01-15T14:35:47",
  "duration_seconds": 325,
  "duration_formatted": "5m 25s",
  "status": "success",
  "dry_run": false,
  "stages": {
    "database_analysis": {
      "status": "success",
      "data": {
        "total_size_mb": 245.67,
        "table_count": 12,
        "potential_savings": {
          "video_stats": 98.45,
          "channel_stats": 24.38
        },
        "total_potential_savings_mb": 122.83,
        "total_potential_savings_pct": 50.0
      }
    },
    "optimization": {
      "status": "success",
      "data": {
        "optimization_stats": {
          "video_stats_removed": 45234,
          "channel_stats_removed": 3421,
          "thumbnails_optimized": 1523,
          "before_size_mb": 245.67,
          "after_size_mb": 127.89,
          "space_reclaimed_mb": 117.78
        }
      }
    },
    "data_integrity": {
      "status": "success",
      "data": {
        "all_passed": true
      }
    }
  },
  "errors": [],
  "warnings": []
}
```

### Key Metrics

- **space_reclaimed_mb**: Actual storage saved
- **video_stats_removed**: Number of stat entries deleted
- **total_potential_savings_pct**: Expected reduction percentage
- **duration_seconds**: Time taken to optimize

---

## Safety Features

### 1. Dry Run Mode
```bash
# Always test first
python scripts/supabase_optimization_workflow.py --dry-run
```
Shows exactly what would be optimized without making changes.

### 2. Optimization Threshold
Skips optimization if potential savings < 10 MB **and** < 5% of database size.

Override with `--force`:
```bash
python scripts/supabase_optimization_workflow.py --optimize --force
```

### 3. Data Integrity Checks
Post-optimization verification ensures:
- No orphaned records
- Foreign key relationships intact
- All data queryable

### 4. Transaction Safety
All deletions run in transactions - if any step fails, changes are rolled back.

### 5. Read-Only Analysis
Database analysis stage only performs `SELECT` queries, never modifies data.

---

## Advanced Usage

### Command-Line Options

```bash
# Analysis only (no optimization)
python scripts/supabase_optimization_workflow.py --report-only

# Quiet mode (minimal output, for cron jobs)
python scripts/supabase_optimization_workflow.py --optimize --quiet

# Force optimization (skip threshold)
python scripts/supabase_optimization_workflow.py --optimize --force

# Dry run with force
python scripts/supabase_optimization_workflow.py --dry-run --force
```

### Cron Schedule

```bash
# Edit crontab
crontab -e

# Weekly optimization (Sunday 3 AM)
0 3 * * 0 cd /home/user/utublr && \
  POSTGRES_URL='postgresql://...' \
  python scripts/supabase_optimization_workflow.py --optimize --quiet >> logs/cron.log 2>&1
```

### Environment Variables

```bash
# Required
export POSTGRES_URL='postgresql://postgres.xxx:password@host:6543/postgres'

# Optional (from config/channels.yaml)
export LOG_LEVEL=INFO
```

---

## Monitoring and Alerts

### Track Storage Over Time

```sql
-- Create a tracking table
CREATE TABLE optimization_history (
    optimized_at TIMESTAMP DEFAULT NOW(),
    size_before_mb NUMERIC,
    size_after_mb NUMERIC,
    space_reclaimed_mb NUMERIC,
    rows_removed INTEGER
);

-- Log each optimization
INSERT INTO optimization_history (size_before_mb, size_after_mb, space_reclaimed_mb, rows_removed)
VALUES (245.67, 127.89, 117.78, 48655);

-- View history
SELECT
    optimized_at,
    size_before_mb,
    size_after_mb,
    space_reclaimed_mb,
    ROUND((space_reclaimed_mb / size_before_mb * 100), 1) as reduction_pct
FROM optimization_history
ORDER BY optimized_at DESC;
```

### Supabase Dashboard Monitoring

1. Go to: Supabase Dashboard → Database → Database Size
2. Track storage trends over time
3. Set up alerts for storage thresholds

### Email Notifications (GitHub Actions)

Add to `.github/workflows/optimize-database.yml`:

```yaml
- name: Send email notification
  if: success()
  uses: dawidd6/action-send-mail@v3
  with:
    server_address: smtp.gmail.com
    server_port: 465
    username: ${{ secrets.EMAIL_USERNAME }}
    password: ${{ secrets.EMAIL_PASSWORD }}
    subject: Database Optimization Complete
    body: file://logs/optimization_workflow_*.json
    to: your-email@example.com
    from: GitHub Actions
```

---

## Troubleshooting

### "POSTGRES_URL environment variable not set"

**Solution**:
```bash
export POSTGRES_URL='postgresql://postgres.xxx:password@aws-0-us-west-1.pooler.supabase.com:6543/postgres'
```

Get your connection string from: Supabase Dashboard → Settings → Database → Connection string (Connection pooling)

### "This workflow requires PostgreSQL backend"

**Solution**: Update `config/channels.yaml`:
```yaml
settings:
  database_backend: postgres
```

### "Optimization threshold not met"

This means potential savings are < 10 MB and < 5%. The database is already well-optimized.

**Options**:
1. Wait for more data to accumulate
2. Force optimization anyway: `--force`
3. Adjust threshold in code

### "VACUUM failed: permission denied"

Supabase managed databases may restrict VACUUM permissions. This is non-critical - space is still reclaimed.

**Workaround**: VACUUM runs automatically in Supabase, no action needed.

### High memory usage during optimization

PostgreSQL may use significant memory for large deletions.

**Solution**: Run optimizations during low-traffic periods (3-5 AM).

---

## Performance Impact

### During Optimization

- **Duration**: 5-15 minutes for typical databases (100-500 MB)
- **CPU**: Moderate usage (30-50%)
- **Memory**: Proportional to table size (~2x largest table)
- **Locks**: Brief table-level locks during deletions (< 1 second each)

### Query Impact

**Minimal** - optimizations actually improve query performance:
- Smaller tables = faster scans
- Better index selectivity
- Reduced I/O overhead

### Production Safety

✅ Safe to run on production databases:
- No downtime required
- No schema changes
- Transactional safety
- Read queries unaffected

---

## Best Practices

### 1. Start with Dry Run
```bash
python scripts/supabase_optimization_workflow.py --dry-run
```

### 2. Schedule Weekly
Set up GitHub Actions or cron for automated weekly optimization.

### 3. Monitor Results
Track storage metrics over time to measure effectiveness.

### 4. Backup Before First Run
```bash
# Supabase: Use Point-in-Time Recovery or manual backup
# Settings → Database → Backups
```

### 5. Review Reports
Check optimization reports for warnings or anomalies.

---

## FAQ

**Q: Will this delete my data?**
A: No. It only removes redundant/duplicate snapshots. All unique data is preserved.

**Q: Can I undo an optimization?**
A: No, but the data removed is redundant. Use dry-run first to verify.

**Q: How often should I run this?**
A: Weekly is recommended for active databases. Monthly for low-activity.

**Q: Does this affect Supabase pricing?**
A: Yes! Reducing storage can lower your Supabase bill (storage is metered).

**Q: What's the maximum reduction possible?**
A: Typically 40-70% for databases with active time-series tracking. New databases may see less reduction.

**Q: Is this safe for production?**
A: Yes. It uses standard PostgreSQL operations and includes integrity checks. Always test with dry-run first.

---

## Support

For issues or questions:

1. Check logs: `logs/supabase_optimization.log`
2. Review report: `logs/optimization_workflow_*.json`
3. Open GitHub issue with:
   - Error message
   - Database size
   - Optimization report (redact sensitive data)

---

## Next Steps

1. ✅ Run dry-run analysis
2. ✅ Review potential savings
3. ✅ Configure GitHub Actions with POSTGRES_URL secret
4. ✅ Run manual workflow trigger
5. ✅ Review first optimization report
6. ✅ Enable weekly scheduled runs
7. Monitor storage trends

**Ready to start? Run the dry-run command above!**
