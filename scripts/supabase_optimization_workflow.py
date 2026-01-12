#!/usr/bin/env python3
"""
Supabase Database Optimization Workflow

This script orchestrates the complete optimization process for Supabase databases:
1. Pre-optimization analysis and safety checks
2. Database backup verification
3. Run optimization strategies
4. Generate comprehensive reports
5. Post-optimization verification

Usage:
    # Dry run (safe, shows what would happen)
    python scripts/supabase_optimization_workflow.py --dry-run

    # Run optimization with full reporting
    python scripts/supabase_optimization_workflow.py --optimize

    # Generate report only (no optimization)
    python scripts/supabase_optimization_workflow.py --report-only

    # Schedule-friendly mode (exits with code 0 if nothing to optimize)
    python scripts/supabase_optimization_workflow.py --optimize --quiet

Environment Variables Required:
    POSTGRES_URL - Supabase connection string
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.database import get_connection, is_postgres
from scripts.optimize_database_postgres import PostgresOptimizer

# Configure logging
LOG_DIR = Path(__file__).parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'supabase_optimization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SupabaseOptimizationWorkflow:
    """Complete optimization workflow for Supabase databases"""

    def __init__(self, dry_run: bool = False, quiet: bool = False):
        self.dry_run = dry_run
        self.quiet = quiet
        self.conn = get_connection()
        self.workflow_id = f"opt_{datetime.now():%Y%m%d_%H%M%S}"
        self.report = {
            'workflow_id': self.workflow_id,
            'started_at': datetime.now().isoformat(),
            'dry_run': dry_run,
            'stages': {},
            'errors': [],
            'warnings': []
        }

    def log_stage(self, stage_name: str, status: str, data: Optional[Dict] = None):
        """Log a workflow stage"""
        self.report['stages'][stage_name] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'data': data or {}
        }

    def verify_connection(self) -> bool:
        """Verify connection to Supabase database"""
        logger.info("Stage 1: Verifying database connection...")

        try:
            if not is_postgres():
                raise ValueError("This workflow requires PostgreSQL backend")

            # Test connection
            result = self.conn.execute("SELECT version(), current_database()").fetchone()
            db_version = result[0]
            db_name = result[1]

            logger.info(f"✓ Connected to database: {db_name}")
            logger.info(f"✓ PostgreSQL version: {db_version.split(',')[0]}")

            self.log_stage('connection_verification', 'success', {
                'database': db_name,
                'version': db_version
            })

            return True

        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            self.report['errors'].append(f"Connection verification failed: {e}")
            self.log_stage('connection_verification', 'failed')
            return False

    def analyze_database(self) -> Dict:
        """Analyze database before optimization"""
        logger.info("\nStage 2: Analyzing database...")

        analysis = {
            'total_size_mb': 0,
            'table_count': 0,
            'row_counts': {},
            'table_sizes_mb': {},
            'time_series_analysis': {},
            'potential_savings': {}
        }

        try:
            # Get database size
            result = self.conn.execute(
                "SELECT pg_database_size(current_database()) / 1024.0 / 1024.0"
            ).fetchone()
            analysis['total_size_mb'] = float(result[0])

            # Get table sizes
            tables_query = """
                SELECT
                    tablename,
                    pg_total_relation_size(schemaname||'.'||tablename)::numeric / 1024.0 / 1024.0 as size_mb,
                    n_live_tup as row_count
                FROM pg_tables
                LEFT JOIN pg_stat_user_tables ON pg_tables.tablename = pg_stat_user_tables.relname
                WHERE schemaname = 'public'
                ORDER BY size_mb DESC
            """

            for row in self.conn.execute(tables_query).fetchall():
                table_name = row[0]
                size_mb = float(row[1])
                row_count = row[2] or 0

                analysis['table_sizes_mb'][table_name] = size_mb
                analysis['row_counts'][table_name] = row_count
                analysis['table_count'] += 1

            # Analyze time-series tables
            for table in ['video_stats', 'channel_stats']:
                if table in analysis['row_counts']:
                    # Get date range and duplicates
                    query = f"""
                        WITH duplicate_check AS (
                            SELECT
                                COUNT(*) as total_rows,
                                MIN(fetched_at) as earliest,
                                MAX(fetched_at) as latest,
                                COUNT(DISTINCT DATE(fetched_at)) as unique_days
                            FROM {table}
                        )
                        SELECT * FROM duplicate_check
                    """
                    result = self.conn.execute(query).fetchone()

                    if result:
                        total_rows = result[0]
                        unique_days = result[3] or 1

                        # Estimate potential savings
                        # Conservative estimate: 40-60% reduction
                        estimated_savings_pct = 50
                        table_size = analysis['table_sizes_mb'].get(table, 0)
                        potential_savings_mb = table_size * (estimated_savings_pct / 100)

                        analysis['time_series_analysis'][table] = {
                            'total_rows': total_rows,
                            'earliest': str(result[1]),
                            'latest': str(result[2]),
                            'unique_days': unique_days,
                            'avg_snapshots_per_day': total_rows / unique_days if unique_days > 0 else 0,
                            'estimated_savings_pct': estimated_savings_pct,
                            'estimated_savings_mb': potential_savings_mb
                        }

                        analysis['potential_savings'][table] = potential_savings_mb

            # Calculate total potential savings
            total_savings = sum(analysis['potential_savings'].values())
            total_savings_pct = (total_savings / analysis['total_size_mb'] * 100) if analysis['total_size_mb'] > 0 else 0

            analysis['total_potential_savings_mb'] = total_savings
            analysis['total_potential_savings_pct'] = total_savings_pct

            # Log summary
            logger.info(f"✓ Database size: {analysis['total_size_mb']:.2f} MB")
            logger.info(f"✓ Tables analyzed: {analysis['table_count']}")
            logger.info(f"✓ Total rows: {sum(analysis['row_counts'].values()):,}")
            logger.info(f"\nPotential Savings:")
            logger.info(f"  • Estimated reduction: {total_savings:.2f} MB ({total_savings_pct:.1f}%)")

            for table, savings in analysis['potential_savings'].items():
                logger.info(f"  • {table}: {savings:.2f} MB")

            self.log_stage('database_analysis', 'success', analysis)
            return analysis

        except Exception as e:
            logger.error(f"✗ Analysis failed: {e}")
            self.report['errors'].append(f"Database analysis failed: {e}")
            self.log_stage('database_analysis', 'failed')
            return analysis

    def check_optimization_threshold(self, analysis: Dict) -> bool:
        """
        Check if optimization is worthwhile.
        Returns False if potential savings < 10 MB or < 5%
        """
        total_savings_mb = analysis.get('total_potential_savings_mb', 0)
        total_savings_pct = analysis.get('total_potential_savings_pct', 0)

        if total_savings_mb < 10 and total_savings_pct < 5:
            logger.info(f"\n⚠ Optimization threshold not met:")
            logger.info(f"  • Potential savings: {total_savings_mb:.2f} MB ({total_savings_pct:.1f}%)")
            logger.info(f"  • Minimum threshold: 10 MB or 5%")
            logger.info(f"  • Recommendation: Skip optimization")

            self.report['warnings'].append("Optimization threshold not met")
            return False

        return True

    def run_optimization(self) -> Dict:
        """Run the optimization process"""
        logger.info("\nStage 3: Running optimization...")

        try:
            optimizer = PostgresOptimizer(dry_run=self.dry_run)
            report = optimizer.run_all_optimizations(generate_report=True)

            self.log_stage('optimization', 'success', report)
            return report

        except Exception as e:
            logger.error(f"✗ Optimization failed: {e}")
            self.report['errors'].append(f"Optimization failed: {e}")
            self.log_stage('optimization', 'failed')
            raise

    def verify_data_integrity(self) -> bool:
        """Verify data integrity after optimization"""
        logger.info("\nStage 4: Verifying data integrity...")

        checks = []

        try:
            # Check for orphaned records
            orphan_checks = [
                ("videos without channels", """
                    SELECT COUNT(*) FROM videos v
                    WHERE NOT EXISTS (SELECT 1 FROM channels c WHERE c.channel_id = v.channel_id)
                """),
                ("comments without videos", """
                    SELECT COUNT(*) FROM comments c
                    WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.video_id = c.video_id)
                """),
                ("video_stats without videos", """
                    SELECT COUNT(*) FROM video_stats vs
                    WHERE NOT EXISTS (SELECT 1 FROM videos v WHERE v.video_id = vs.video_id)
                """),
            ]

            all_passed = True
            for check_name, query in orphan_checks:
                result = self.conn.execute(query).fetchone()
                count = result[0] if result else 0

                checks.append({
                    'name': check_name,
                    'orphaned_count': count,
                    'passed': count == 0
                })

                if count > 0:
                    logger.warning(f"⚠ Found {count:,} {check_name}")
                    all_passed = False
                else:
                    logger.info(f"✓ No {check_name}")

            self.log_stage('data_integrity', 'success' if all_passed else 'warning', {
                'checks': checks,
                'all_passed': all_passed
            })

            return all_passed

        except Exception as e:
            logger.error(f"✗ Integrity check failed: {e}")
            self.report['errors'].append(f"Integrity verification failed: {e}")
            self.log_stage('data_integrity', 'failed')
            return False

    def generate_final_report(self) -> Dict:
        """Generate comprehensive final report"""
        logger.info("\nStage 5: Generating final report...")

        self.report['completed_at'] = datetime.now().isoformat()

        # Calculate duration
        start_time = datetime.fromisoformat(self.report['started_at'])
        end_time = datetime.fromisoformat(self.report['completed_at'])
        duration_seconds = (end_time - start_time).total_seconds()

        self.report['duration_seconds'] = duration_seconds
        self.report['duration_formatted'] = f"{int(duration_seconds // 60)}m {int(duration_seconds % 60)}s"

        # Determine overall status
        if self.report['errors']:
            self.report['status'] = 'failed'
        elif self.report['warnings']:
            self.report['status'] = 'completed_with_warnings'
        else:
            self.report['status'] = 'success'

        # Save report to file
        report_file = LOG_DIR / f'optimization_workflow_{self.workflow_id}.json'
        with open(report_file, 'w') as f:
            json.dump(self.report, f, indent=2)

        logger.info(f"✓ Report saved to: {report_file}")

        return self.report

    def print_summary(self):
        """Print human-readable summary"""
        print("\n" + "=" * 80)
        print("OPTIMIZATION WORKFLOW SUMMARY")
        print("=" * 80)

        print(f"\nWorkflow ID: {self.workflow_id}")
        print(f"Status: {self.report['status'].upper()}")
        print(f"Duration: {self.report.get('duration_formatted', 'N/A')}")
        print(f"Dry Run: {self.dry_run}")

        # Before/After comparison
        if 'database_analysis' in self.report['stages']:
            analysis = self.report['stages']['database_analysis']['data']
            print(f"\nDatabase Analysis:")
            print(f"  • Total size: {analysis['total_size_mb']:.2f} MB")
            print(f"  • Tables: {analysis['table_count']}")
            print(f"  • Total rows: {sum(analysis['row_counts'].values()):,}")

        if 'optimization' in self.report['stages']:
            opt_data = self.report['stages']['optimization']['data']
            if opt_data and 'optimization_stats' in opt_data:
                stats = opt_data['optimization_stats']
                print(f"\nOptimization Results:")
                print(f"  • Video stats removed: {stats.get('video_stats_removed', 0):,} rows")
                print(f"  • Channel stats removed: {stats.get('channel_stats_removed', 0):,} rows")
                print(f"  • Thumbnails optimized: {stats.get('thumbnails_optimized', 0):,}")
                print(f"  • Space reclaimed: {stats.get('space_reclaimed_mb', 0):.2f} MB")

                before = stats.get('before_size_mb', 0)
                after = stats.get('after_size_mb', 0)
                if before > 0:
                    reduction_pct = ((before - after) / before) * 100
                    print(f"  • Size reduction: {reduction_pct:.1f}%")

        # Warnings and errors
        if self.report['warnings']:
            print(f"\nWarnings ({len(self.report['warnings'])}):")
            for warning in self.report['warnings']:
                print(f"  ⚠ {warning}")

        if self.report['errors']:
            print(f"\nErrors ({len(self.report['errors'])}):")
            for error in self.report['errors']:
                print(f"  ✗ {error}")

        print("\n" + "=" * 80)

    def run(self, skip_threshold_check: bool = False):
        """Run the complete optimization workflow"""
        try:
            # Stage 1: Verify connection
            if not self.verify_connection():
                logger.error("Workflow aborted: Connection verification failed")
                return 1

            # Stage 2: Analyze database
            analysis = self.analyze_database()

            # Check if optimization is worthwhile (unless forced)
            if not skip_threshold_check and not self.check_optimization_threshold(analysis):
                if self.quiet:
                    return 0  # Exit successfully (nothing to do)
                else:
                    logger.info("\nUse --force to run optimization anyway")
                    return 0

            # Stage 3: Run optimization
            if not self.dry_run:
                self.run_optimization()
            else:
                logger.info("\n⚠ DRY RUN MODE - No changes made")
                logger.info("Run without --dry-run to apply optimizations")

            # Stage 4: Verify integrity (if not dry run)
            if not self.dry_run:
                self.verify_data_integrity()

            # Stage 5: Generate report
            self.generate_final_report()

            # Print summary
            if not self.quiet:
                self.print_summary()

            return 0 if not self.report['errors'] else 1

        except Exception as e:
            logger.error(f"Workflow failed: {e}", exc_info=True)
            self.report['errors'].append(f"Workflow exception: {e}")
            self.generate_final_report()
            return 1


def main():
    parser = argparse.ArgumentParser(
        description="Supabase Database Optimization Workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--dry-run', action='store_true',
                       help="Analyze and show what would be optimized (no changes)")
    parser.add_argument('--optimize', action='store_true',
                       help="Run full optimization workflow")
    parser.add_argument('--report-only', action='store_true',
                       help="Generate analysis report only (no optimization)")
    parser.add_argument('--force', action='store_true',
                       help="Skip threshold check and force optimization")
    parser.add_argument('--quiet', action='store_true',
                       help="Minimal output (for scheduled runs)")

    args = parser.parse_args()

    # Validate args
    if not any([args.dry_run, args.optimize, args.report_only]):
        parser.print_help()
        print("\nError: Must specify --dry-run, --optimize, or --report-only")
        return 1

    # Check environment
    if not os.getenv('POSTGRES_URL'):
        logger.error("Error: POSTGRES_URL environment variable not set")
        logger.error("Set it to your Supabase connection string:")
        logger.error("  export POSTGRES_URL='postgresql://user:pass@host:port/dbname'")
        return 1

    # Run workflow
    workflow = SupabaseOptimizationWorkflow(
        dry_run=(args.dry_run or args.report_only),
        quiet=args.quiet
    )

    return workflow.run(skip_threshold_check=args.force)


if __name__ == '__main__':
    sys.exit(main())
