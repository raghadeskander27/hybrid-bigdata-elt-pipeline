"""
MongoDB setup and index initialization module.
Includes Phase 1 core indices and Phase 2 query optimization / compound indices.
"""
from typing import Optional, List, Dict, Any
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database

from config.settings import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    JOB_LOGS_COLLECTION,
    VIEW_METADATA_COLLECTION,
)

# Phase 2 Indexes definition for easy toggling during explain benchmarks
PHASE2_INDEXES: List[Dict[str, Any]] = [
    {
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "name": "idx_compound_customer_order_date",
        "description": "Compound index supporting customer order history lookup sorted chronologically",
    },
    {
        "keys": [("total_amount", DESCENDING)],
        "name": "idx_total_amount_desc",
        "description": "Single-field range index for high-value orders filtering and sorting",
    },
    {
        "keys": [("items.item_id", ASCENDING)],
        "name": "idx_items_item_id",
        "description": "Multikey array index for fast lookup of orders containing specific product item_id",
    },
]


def get_mongo_client(uri: Optional[str] = None) -> MongoClient:
    """Creates and returns a MongoClient instance."""
    connection_uri = uri or MONGO_URI
    return MongoClient(connection_uri)


def get_database(client: Optional[MongoClient] = None, db_name: Optional[str] = None) -> Database:
    """Returns the database instance."""
    mongo_client = client or get_mongo_client()
    target_db = db_name or DB_NAME
    return mongo_client[target_db]


def create_phase2_indexes(db: Database) -> List[str]:
    """Creates the 3 required Phase 2 indexes (including compound index) on orders_validated."""
    val_col = db[VALIDATED_COLLECTION]
    created = []
    for idx_def in PHASE2_INDEXES:
        name = val_col.create_index(idx_def["keys"], name=idx_def["name"])
        created.append(name)
    return created


def drop_phase2_indexes(db: Database) -> List[str]:
    """Drops the Phase 2 indexes on orders_validated (used for explain benchmark comparisons)."""
    val_col = db[VALIDATED_COLLECTION]
    existing = val_col.index_information()
    dropped = []
    for idx_def in PHASE2_INDEXES:
        name = idx_def["name"]
        if name in existing:
            val_col.drop_index(name)
            dropped.append(name)
    return dropped


def init_db(client: Optional[MongoClient] = None, db_name: Optional[str] = None) -> Database:
    """
    Ensures required collections exist and creates necessary indexes:
    - orders_raw: indexed by run_id, ingested_at
    - orders_validated:
        - UNIQUE index on order_id (business key)
        - index on customer_id, order_date
        - Phase 2 compound and specialized indexes
    - orders_quarantine: indexed by run_id, error_code
    - materialized views: unique indices on day and item_id
    - job_logs: indexed by job_name, start_time
    """
    db = get_database(client, db_name)

    # 1. orders_raw indexes
    raw_col = db[RAW_COLLECTION]
    raw_col.create_index([("run_id", ASCENDING)], name="idx_raw_run_id")
    raw_col.create_index([("ingested_at", ASCENDING)], name="idx_raw_ingested_at")

    # 2. orders_validated indexes (STRICT UNIQUE INDEX ON order_id)
    val_col = db[VALIDATED_COLLECTION]
    val_col.create_index([("order_id", ASCENDING)], unique=True, name="uniq_validated_order_id")
    val_col.create_index([("customer_id", ASCENDING)], name="idx_val_customer_id")
    val_col.create_index([("order_date", ASCENDING)], name="idx_val_order_date")
    val_col.create_index([("run_id", ASCENDING)], name="idx_val_run_id")

    # Phase 2 Indexes
    create_phase2_indexes(db)

    # 3. orders_quarantine indexes
    quarantine_col = db[QUARANTINE_COLLECTION]
    quarantine_col.create_index([("run_id", ASCENDING)], name="idx_quarantine_run_id")
    quarantine_col.create_index([("error_code", ASCENDING)], name="idx_quarantine_error_code")

    # 4. Materialized Views indexes
    daily_mv = db[MV_DAILY_SALES]
    daily_mv.create_index([("day", ASCENDING)], unique=True, name="uniq_mv_day")

    products_mv = db[MV_TOP_PRODUCTS]
    products_mv.create_index([("item_id", ASCENDING)], unique=True, name="uniq_mv_item_id")

    # 5. Job Logs & Metadata
    job_logs = db[JOB_LOGS_COLLECTION]
    job_logs.create_index([("job_name", ASCENDING), ("start_time", DESCENDING)], name="idx_job_logs_name_time")

    meta_col = db[VIEW_METADATA_COLLECTION]
    meta_col.create_index([("view_name", ASCENDING)], unique=True, name="uniq_meta_view_name")

    return db
