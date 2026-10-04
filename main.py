"""
Main execution CLI entrypoint for the Hybrid Big Data ELT Pipeline & Phase 2 Services.
Supports pipeline ingestion, explain benchmarks, aggregations, materialized views,
scheduled job triggers, and launching the unified FastAPI server.
"""
import argparse
import json
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import (
    DEFAULT_DIRTY_DATASET_PATH,
    DB_NAME,
    MONGO_URI,
    RESULTS_FILE,
    API_HOST,
    API_PORT,
)
from src.create_small_sample import create_dirty_sample_csv
from src.elt_pipeline import run_pipeline
from src.mongo_setup import get_database, init_db
from src.explain_runner import run_explain_comparison, print_explain_report
from src.aggregations import AVAILABLE_AGGREGATIONS, run_aggregation
from src.materialized_views import refresh_all_materialized_views, get_materialized_view_data
from src.scheduler import REGISTERED_JOBS, run_job_manually


def parse_args():
    parser = argparse.ArgumentParser(
        description="Hybrid Big Data ELT Pipeline & Phase 2 Platform"
    )
    # Pipeline Ingestion arguments
    parser.add_argument(
        "--file",
        type=str,
        default=str(DEFAULT_DIRTY_DATASET_PATH),
        help="Path to the input CSV file to process.",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["auto", "batch", "spark"],
        default="auto",
        help="Engine to use: 'auto' (threshold router), 'batch' (Python DictReader), or 'spark' (PySpark).",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=DB_NAME,
        help=f"Target MongoDB database name (default: {DB_NAME}).",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default=MONGO_URI,
        help="MongoDB connection URI.",
    )
    parser.add_argument(
        "--generate-sample",
        action="store_true",
        help="Generate synthetic dirty sample data before executing.",
    )

    # Phase 2 Action Flags
    parser.add_argument(
        "--serve-api",
        action="store_true",
        help="Start the unified FastAPI server with Swagger docs on /docs.",
    )
    parser.add_argument(
        "--api-host",
        type=str,
        default=API_HOST,
        help=f"FastAPI host (default: {API_HOST}).",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=API_PORT,
        help=f"FastAPI port (default: {API_PORT}).",
    )
    parser.add_argument(
        "--run-explain",
        action="store_true",
        help="Run explain('executionStats') benchmark on 3 queries BEFORE and AFTER indexes.",
    )
    parser.add_argument(
        "--run-aggregations",
        action="store_true",
        help="Execute all 5 analytical aggregation reports and print outputs.",
    )
    parser.add_argument(
        "--refresh-mv",
        action="store_true",
        help="Trigger incremental refresh of Materialized Views.",
    )
    parser.add_argument(
        "--run-job",
        type=str,
        choices=list(REGISTERED_JOBS.keys()),
        help="Manually trigger a registered background job by name.",
    )
    return parser.parse_args()


def print_banner():
    print("=" * 70)
    print("      HYBRID BIG DATA ELT PIPELINE (Batch / Spark + MongoDB)     ")
    print("=" * 70)


def print_summary_table(metrics: dict):
    print("\n" + "-" * 70)
    print("                     EXECUTION SUMMARY METRICS                     ")
    print("-" * 70)
    print(f" Run ID                    : {metrics['run_id']}")
    print(f" Input File                : {metrics['file_path']}")
    print(f" File Size                 : {metrics['file_size_mb']} MB")
    print(f" Engine Selected           : {metrics['engine_used']}")
    print(f" Duration                  : {metrics['duration_seconds']} seconds")
    print("-" * 70)
    print(f" Raw Records Ingested      : {metrics['run_raw_count']}")
    print(f" Valid Records (Clean)     : {metrics['valid_count']}")
    print(f" Corrected Records         : {metrics['corrected_count']}")
    print(f" Quarantined Records       : {metrics['quarantine_count']}")
    print(f" Total Validated Processed : {metrics['validated_total']}")
    print("-" * 70)
    print(f" MongoDB Upserted (New)    : {metrics['inserted_count']}")
    print(f" MongoDB Matched (Existing): {metrics['matched_count']}")
    print(f" MongoDB Unchanged         : {metrics['unchanged_count']}")
    print(f" Quarantine Records Stored : {metrics['quarantine_inserted_count']}")
    print("-" * 70)
    print(f" Consistency Invariant Met : {'PASSED' if metrics['consistency_invariant_met'] else 'FAILED'}")
    print(f" Metrics Appended To       : {RESULTS_FILE}")
    print("=" * 70 + "\n")


def execute_explain_action(db_name: str):
    print("\n--- Running Explain Analysis (Before vs After Indexes) ---")
    db = get_database(db_name=db_name)
    report = run_explain_comparison(db)
    print_explain_report(report)


def execute_aggregations_action(db_name: str):
    print("\n--- Executing 5 Aggregation Reports ---")
    db = get_database(db_name=db_name)
    for name, meta in AVAILABLE_AGGREGATIONS.items():
        print(f"\n[REPORT] {name.upper()}: {meta['description']}")
        print("-" * 70)
        results = run_aggregation(db, name)
        print(json.dumps(results, indent=2, ensure_ascii=False))


def execute_mv_action(db_name: str):
    print("\n--- Refreshing Materialized Views (Incremental) ---")
    db = get_database(db_name=db_name)
    result = refresh_all_materialized_views(db)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def execute_job_action(job_name: str, db_name: str):
    print(f"\n--- Manually Triggering Job: '{job_name}' ---")
    db = get_database(db_name=db_name)
    result = run_job_manually(job_name, db=db)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def start_api_server(host: str, port: int):
    import uvicorn
    print(f"\nStarting FastAPI server on http://{host}:{port} ...")
    print(f"Swagger Documentation available at: http://{host}:{port}/docs\n")
    uvicorn.run("src.api:app", host=host, port=port, reload=False)


def main():
    args = parse_args()
    print_banner()

    # 1. Action: Serve API
    if args.serve_api:
        start_api_server(args.api_host, args.api_port)
        return

    # 2. Action: Run Explain Comparison
    if args.run_explain:
        execute_explain_action(args.db_name)
        return

    # 3. Action: Run Aggregations
    if args.run_aggregations:
        execute_aggregations_action(args.db_name)
        return

    # 4. Action: Refresh Materialized Views
    if args.refresh_mv:
        execute_mv_action(args.db_name)
        return

    # 5. Action: Run Job Manually
    if args.run_job:
        execute_job_action(args.run_job, args.db_name)
        return

    # Default Action: Standard ELT Pipeline Ingestion
    input_path = Path(args.file)

    if args.generate_sample or not input_path.exists():
        print(f"Dataset not found or --generate-sample requested. Generating dataset at: {input_path}...")
        create_dirty_sample_csv(input_path)
        print("Sample dataset generation complete.\n")

    print(f"Starting ELT pipeline execution on: {input_path}")
    print(f"Target Database : {args.db_name}")
    print(f"Engine Mode     : {args.engine.upper()}\n")

    try:
        metrics = run_pipeline(
            file_path=input_path,
            engine=args.engine,
            db_name=args.db_name,
            mongo_uri=args.mongo_uri,
        )
        print_summary_table(metrics)
    except Exception as e:
        print(f"\n[ERROR] Pipeline execution failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
