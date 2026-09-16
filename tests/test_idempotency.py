"""
Idempotency and consistency invariant test suite.
Verifies that re-running the exact same batch yields inserted_count == 0,
all existing valid records remain deduplicated, and consistency invariant holds.
"""
import pytest
from pathlib import Path
from src.mongo_setup import get_mongo_client, get_database, init_db
from src.elt_pipeline import run_pipeline
from src.create_small_sample import create_dirty_sample_csv
from config.settings import VALIDATED_COLLECTION, RAW_COLLECTION, QUARANTINE_COLLECTION

TEST_DB_NAME = "bigdata_elt_idempotency_test"


@pytest.fixture(scope="module")
def mongo_test_db():
    """Initializes and tears down a clean test MongoDB database."""
    client = get_mongo_client()
    db = client[TEST_DB_NAME]
    # Clean prior state
    client.drop_database(TEST_DB_NAME)
    init_db(client, db_name=TEST_DB_NAME)
    yield db
    # Cleanup after tests
    client.drop_database(TEST_DB_NAME)
    client.close()


@pytest.fixture(scope="module")
def sample_csv(tmp_path_factory):
    """Generates a small test dataset in a temporary directory."""
    temp_dir = tmp_path_factory.mktemp("data")
    csv_file = temp_dir / "sample_batch.csv"
    create_dirty_sample_csv(target_path=csv_file)
    return csv_file


def test_idempotent_pipeline_execution(mongo_test_db, sample_csv):
    """
    Test that executing the pipeline on the same dataset twice:
    1. First run inserts all valid records (inserted_count == validated_total).
    2. Second run results in inserted_count == 0 and unchanged_count == validated_total.
    3. No duplicate order_id exists in orders_validated.
    4. Consistency invariant (run_raw_count == valid + corrected + quarantine) holds on both runs.
    """
    val_col = mongo_test_db[VALIDATED_COLLECTION]
    raw_col = mongo_test_db[RAW_COLLECTION]

    # --- Run 1: Initial Ingestion ---
    metrics_run1 = run_pipeline(
        file_path=sample_csv,
        engine="batch",
        db_name=TEST_DB_NAME,
        record_metrics=False,
    )

    total_validated_1 = metrics_run1["validated_total"]
    assert total_validated_1 > 0, "Initial run should validate records"
    assert metrics_run1["inserted_count"] == total_validated_1
    assert metrics_run1["consistency_invariant_met"] is True
    assert metrics_run1["run_raw_count"] == (
        metrics_run1["valid_count"] + metrics_run1["corrected_count"] + metrics_run1["quarantine_count"]
    )

    count_after_run1 = val_col.count_documents({})
    assert count_after_run1 == total_validated_1

    # --- Run 2: Exact Duplicate Re-run ---
    metrics_run2 = run_pipeline(
        file_path=sample_csv,
        engine="batch",
        db_name=TEST_DB_NAME,
        record_metrics=False,
    )

    assert metrics_run2["inserted_count"] == 0, (
        f"Second run should have 0 inserts, got: {metrics_run2['inserted_count']}"
    )
    assert metrics_run2["matched_count"] == total_validated_1
    assert metrics_run2["unchanged_count"] == total_validated_1
    assert metrics_run2["consistency_invariant_met"] is True

    # Verify no duplicate order_id records exist
    count_after_run2 = val_col.count_documents({})
    assert count_after_run2 == total_validated_1, (
        f"Duplicate orders detected! Expected {total_validated_1} documents, found {count_after_run2}"
    )

    # Validate with aggregation that all order_id counts are strictly 1
    pipeline_check = [
        {"$group": {"_id": "$order_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    duplicates = list(val_col.aggregate(pipeline_check))
    assert len(duplicates) == 0, f"Found duplicate order_ids in orders_validated: {duplicates}"
