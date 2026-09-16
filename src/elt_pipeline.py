"""
End-to-End ELT Pipeline orchestrator.
Manages raw ingestion, quality validation, idempotent upserting,
quarantine management, consistency invariant assertions, and metrics reporting.
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pymongo import UpdateOne
from pymongo.database import Database

from config.settings import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    RESULTS_FILE,
    REPORTS_DIR,
)
from src.mongo_setup import init_db, get_mongo_client
from src.file_router import route_file, get_file_size_mb
from src.batch_loader import load_raw_batch
from src.spark_loader import load_raw_spark
from src.quality_rules import validate_record


def append_metrics_to_report(metrics: Dict[str, Any], results_file: Path = RESULTS_FILE) -> None:
    """Appends run metrics to reports/results.json."""
    results_file.parent.mkdir(parents=True, exist_ok=True)
    existing_reports: List[Dict[str, Any]] = []

    if results_file.exists() and results_file.stat().st_size > 0:
        try:
            with open(results_file, "r", encoding="utf-8") as f:
                content = json.load(f)
                if isinstance(content, list):
                    existing_reports = content
                elif isinstance(content, dict):
                    existing_reports = [content]
        except Exception:
            existing_reports = []

    existing_reports.append(metrics)
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(existing_reports, f, indent=2, ensure_ascii=False)


def run_pipeline(
    file_path: Union[str, Path],
    engine: str = "auto",
    db_name: Optional[str] = None,
    mongo_uri: Optional[str] = None,
    run_id: Optional[str] = None,
    record_metrics: bool = True,
) -> Dict[str, Any]:
    """
    Executes the complete ELT pipeline:
    1. Routes file to python_batch or pyspark.
    2. Ingests 100% of raw records into orders_raw.
    3. Transforms, validates, and logs audit trail.
    4. Upserts validated records into orders_validated (strictly idempotent).
    5. Stores quarantined records in orders_quarantine.
    6. Asserts consistency invariant: run_raw_count == valid_count + corrected_count + quarantine_count.
    7. Appends run metrics to reports/results.json.

    Returns:
        Summary metrics dictionary.
    """
    start_time_dt = datetime.now(timezone.utc)
    t0 = time.time()

    resolved_path = Path(file_path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Input file not found: {resolved_path}")

    current_run_id = run_id or f"run_{start_time_dt.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    target_db_name = db_name or DB_NAME
    target_uri = mongo_uri or MONGO_URI

    # 1. Initialize MongoDB Database and Unique / Query Indexes
    client = get_mongo_client(target_uri)
    db: Database = init_db(client, db_name=target_db_name)

    # 2. File Routing
    file_size_mb = get_file_size_mb(resolved_path)
    if engine.lower() == "auto":
        chosen_engine = route_file(resolved_path)
    elif engine.lower() in ("batch", "python_batch"):
        chosen_engine = "python_batch"
    elif engine.lower() in ("spark", "pyspark"):
        chosen_engine = "pyspark"
    else:
        raise ValueError(f"Unsupported engine option: {engine}. Choose 'auto', 'batch', or 'spark'.")

    # 3. Raw Data Ingestion (ELT: Load first, no filtering)
    if chosen_engine == "python_batch":
        ingested_count = load_raw_batch(resolved_path, run_id=current_run_id, db=db)
    else:
        ingested_count = load_raw_spark(
            resolved_path,
            run_id=current_run_id,
            mongo_uri=target_uri,
            db_name=target_db_name,
        )

    raw_col = db[RAW_COLLECTION]
    run_raw_count = raw_col.count_documents({"run_id": current_run_id})

    # 4. Transform & Quality Validation
    cursor = raw_col.find({"run_id": current_run_id})
    validated_records = []
    quarantine_records = []

    valid_count = 0      # Clean records with zero corrections
    corrected_count = 0  # Valid records with one or more audit corrections
    quarantine_count = 0 # Invalid records

    for raw_doc in cursor:
        raw_rec = raw_doc.get("raw_record", {})
        is_valid, processed_doc, corrections_cnt, err_code, err_details = validate_record(raw_rec, current_run_id)

        if is_valid:
            if corrections_cnt == 0:
                valid_count += 1
            else:
                corrected_count += 1
            # Add validated metadata
            processed_doc["validated_at"] = datetime.now(timezone.utc).isoformat()
            validated_records.append(processed_doc)
        else:
            quarantine_count += 1
            processed_doc["quarantined_at"] = datetime.now(timezone.utc).isoformat()
            quarantine_records.append(processed_doc)

    # 5. Consistency Invariant Assertion
    # run_raw_count MUST strictly equal valid_count + corrected_count + quarantine_count
    total_processed = valid_count + corrected_count + quarantine_count
    assert run_raw_count == total_processed, (
        f"Consistency Invariant Violated! "
        f"run_raw_count ({run_raw_count}) != valid ({valid_count}) + corrected ({corrected_count}) + quarantine ({quarantine_count})"
    )

    # 6. Idempotent Upsert into orders_validated
    val_col = db[VALIDATED_COLLECTION]
    inserted_count = 0
    matched_count = 0
    modified_count = 0

    if validated_records:
        operations = [
            UpdateOne(
                {"order_id": doc["order_id"]},
                {
                    "$set": {
                        "customer_id": doc["customer_id"],
                        "phone": doc["phone"],
                        "items": doc["items"],
                        "order_date": doc["order_date"],
                        "total_amount": doc["total_amount"],
                        "corrections": doc["corrections"],
                    },
                    "$setOnInsert": {
                        "order_id": doc["order_id"],
                        "run_id": doc["run_id"],
                        "validated_at": doc["validated_at"],
                    },
                },
                upsert=True,
            )
            for doc in validated_records
        ]
        val_result = val_col.bulk_write(operations, ordered=False)
        inserted_count = val_result.upserted_count
        matched_count = val_result.matched_count
        modified_count = val_result.modified_count

    # 7. Write to orders_quarantine
    quar_col = db[QUARANTINE_COLLECTION]
    quarantine_inserted_count = 0
    if quarantine_records:
        # Upsert quarantine records by (run_id, order_id, error_code)
        quar_ops = [
            UpdateOne(
                {
                    "run_id": q_doc["run_id"],
                    "order_id": q_doc.get("order_id"),
                    "error_code": q_doc["error_code"],
                },
                {
                    "$set": {
                        "error_details": q_doc["error_details"],
                        "raw_record": q_doc["raw_record"],
                    },
                    "$setOnInsert": {
                        "run_id": q_doc["run_id"],
                        "order_id": q_doc.get("order_id"),
                        "error_code": q_doc["error_code"],
                        "quarantined_at": q_doc["quarantined_at"],
                    },
                },
                upsert=True,
            )
            for q_doc in quarantine_records
        ]
        q_result = quar_col.bulk_write(quar_ops, ordered=False)
        quarantine_inserted_count = q_result.upserted_count

    end_time_dt = datetime.now(timezone.utc)
    duration_sec = round(time.time() - t0, 3)

    metrics = {
        "run_id": current_run_id,
        "file_path": str(resolved_path),
        "file_size_mb": round(file_size_mb, 4),
        "engine_used": chosen_engine,
        "run_raw_count": run_raw_count,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "validated_total": valid_count + corrected_count,
        "inserted_count": inserted_count,
        "matched_count": matched_count,
        "modified_count": modified_count,
        "unchanged_count": matched_count - modified_count,
        "quarantine_inserted_count": quarantine_inserted_count,
        "consistency_invariant_met": True,
        "start_time": start_time_dt.isoformat(),
        "end_time": end_time_dt.isoformat(),
        "duration_seconds": duration_sec,
    }

    if record_metrics:
        append_metrics_to_report(metrics)

    return metrics
