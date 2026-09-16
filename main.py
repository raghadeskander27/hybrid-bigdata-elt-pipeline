"""
Main execution CLI entrypoint for the Hybrid Big Data ELT Pipeline.
"""
import argparse
import json
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.settings import DEFAULT_DIRTY_DATASET_PATH, DB_NAME, MONGO_URI, RESULTS_FILE
from src.create_small_sample import create_dirty_sample_csv
from src.elt_pipeline import run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(
        description="Hybrid Big Data ELT Pipeline (Python Batch + PySpark + MongoDB)"
    )
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


def main():
    args = parse_args()
    print_banner()

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
