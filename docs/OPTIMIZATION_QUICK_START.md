# Database Optimization - Quick Start Guide

Complete guide to reducing your database storage by 40-70% with automated workflows.

---

## 🚀 For Local SQLite Database

### One-Time Optimization

```bash
# 1. Dry run (see what would be optimized)
python scripts/optimize_database.py --dry-run --all

# 2. Run optimization
python scripts/optimize_database.py --all
```

### Scheduled Optimization

```bash
# Daily at 2 AM
0 2 * * * cd /home/user/utublr && python scripts/scheduled_optimization.py --daily

# Weekly on Sunday at 3 AM
0 3 * * 0 cd /home/user/utublr && python scripts/scheduled_optimization.py --weekly

# Monthly on 1st at 4 AM
0 4 1 * * cd /home/user/utublr && python scripts/scheduled_optimization.py --monthly
```

**Guide**: `/docs/DATABASE_OPTIMIZATION_GUIDE.md`

---

## ☁️ For Supabase PostgreSQL Database

### Setup (One-Time)

```bash
# 1. Get your Supabase connection string
# Dashboard → Settings → Database → Connection string (Connection pooling)

# 2. Set environment variable
export POSTGRES_URL='postgresql://postgres.xxx:password@aws-0-us-west-1.pooler.supabase.com:6543/postgres'

# 3. Update config
# Edit config/channels.yaml:
#   database_backend: postgres
```

### Run Optimization

```bash
# 1. Dry run (analysis only)
python scripts/supabase_optimization_workflow.py --dry-run

# 2. Run optimization
python scripts/supabase_optimization_workflow.py --optimize

# 3. View report
cat logs/optimization_workflow_*.json
```

### GitHub Actions Automation

**Setup**:
1. Go to: Repository → Settings → Secrets → Actions
2. Add secret: `POSTGRES_URL` = your Supabase connection string
3. Done! Workflow runs automatically every Sunday at 3 AM UTC

**Manual Run**:
1. Go to: Repository → Actions → "Database Optimization"
2. Click "Run workflow"
3. Select options:
   - ✅ **Dry run mode** (for analysis only)
   - ⬜ **Force optimization** (skip threshold check)
4. Click "Run workflow"
5. View results in the workflow summary

**Guide**: `/docs/SUPABASE_OPTIMIZATION.md`

---

## 📊 What Gets Optimized?

### 1. Time-Series Compression (40-60% reduction)

Removes redundant snapshots and aggregates old data:

| Age | Granularity | Example |
|-----|-------------|---------|
| 0-30 days | Hourly | Keep all 720 snapshots |
| 30-90 days | Daily | Keep 60 snapshots (92% reduction) |
| 90-365 days | Weekly | Keep 40 snapshots (86% reduction) |
| > 1 year | Monthly | Keep 12 snapshots/year (97% reduction) |

### 2. Duplicate Removal (10-20% reduction)

Removes consecutive stats entries where values didn't change:

```
Before: [1000 views, 1000 views, 1000 views, 1001 views]
After:  [1000 views, 1001 views]
```

### 3. Thumbnail Optimization (2-5% reduction)

Stores quality indicator instead of full URL:

```
Before: "https://i.ytimg.com/vi/VIDEO_ID/maxresdefault.jpg" (57 bytes)
After:  "maxresdefault" (13 bytes)
```

---

## 📈 Expected Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Database size | 500 MB | 200-300 MB | 40-60% |
| video_stats rows | 100,000 | 30,000-50,000 | 50-70% |
| Query performance | Baseline | Same or faster | 0-30% |

---

## 🛡️ Safety Features

✅ **Dry-run mode** - Test without changes
✅ **Transaction safety** - Rollback on errors
✅ **Data integrity checks** - Verify after optimization
✅ **Threshold validation** - Skip if not worthwhile
✅ **Comprehensive logging** - Track all changes
✅ **Production-safe** - No downtime required

---

## 📁 File Reference

| File | Purpose |
|------|---------|
| `scripts/optimize_database.py` | SQLite optimizer |
| `scripts/optimize_database_postgres.py` | PostgreSQL optimizer |
| `scripts/scheduled_optimization.py` | Automated maintenance |
| `scripts/supabase_optimization_workflow.py` | Complete workflow with reporting |
| `.github/workflows/optimize-database.yml` | GitHub Actions automation |
| `docs/DATABASE_OPTIMIZATION_GUIDE.md` | SQLite guide |
| `docs/SUPABASE_OPTIMIZATION.md` | Supabase guide |
| `docs/database_optimization_plan.md` | Technical specification |

---

## 🔍 Common Commands

```bash
# SQLite: Dry run
python scripts/optimize_database.py --dry-run --all

# SQLite: Optimize
python scripts/optimize_database.py --all

# PostgreSQL: Dry run
python scripts/supabase_optimization_workflow.py --dry-run

# PostgreSQL: Optimize
python scripts/supabase_optimization_workflow.py --optimize

# PostgreSQL: Report only
python scripts/supabase_optimization_workflow.py --report-only

# PostgreSQL: Force optimization (skip threshold)
python scripts/supabase_optimization_workflow.py --optimize --force

# View logs
tail -f logs/optimization.log
tail -f logs/supabase_optimization.log

# View reports
ls -lt logs/optimization_workflow_*.json | head -1
cat $(ls -t logs/optimization_workflow_*.json | head -1)
```

---

## 🆘 Troubleshooting

### "POSTGRES_URL environment variable not set"
```bash
export POSTGRES_URL='postgresql://...'
```

### "This workflow requires PostgreSQL backend"
Update `config/channels.yaml`:
```yaml
settings:
  database_backend: postgres
```

### "Optimization threshold not met"
Use `--force` to override:
```bash
python scripts/supabase_optimization_workflow.py --optimize --force
```

### Check database size
```bash
# SQLite
du -sh data/youtube.db

# PostgreSQL
python -c "
from scripts.database import get_cursor
c = get_cursor()
size = c.execute('SELECT pg_database_size(current_database()) / 1024.0 / 1024.0').fetchone()[0]
print(f'Database size: {size:.2f} MB')
"
```

---

## 📞 Support

- **SQLite issues**: See `/docs/DATABASE_OPTIMIZATION_GUIDE.md`
- **Supabase issues**: See `/docs/SUPABASE_OPTIMIZATION.md`
- **Technical details**: See `/docs/database_optimization_plan.md`
- **GitHub Issues**: Report problems with logs and reports attached

---

## ✅ Next Steps

### For Local SQLite:
1. Run dry-run: `python scripts/optimize_database.py --dry-run --all`
2. Review output
3. Run optimization: `python scripts/optimize_database.py --all`
4. Set up cron for automated maintenance

### For Supabase:
1. Set POSTGRES_URL environment variable
2. Run dry-run: `python scripts/supabase_optimization_workflow.py --dry-run`
3. Review report
4. Add POSTGRES_URL to GitHub secrets
5. Enable GitHub Actions workflow
6. Monitor weekly automated runs

**Ready? Pick your database type above and follow the steps!**
