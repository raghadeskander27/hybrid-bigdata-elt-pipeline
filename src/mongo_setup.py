"""
MongoDB setup and index initialization module.
"""
from typing import Optional
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database

from config.settings import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_mongo_client(uri: Optional[str] = None) -> MongoClient:
    """Creates and returns a MongoClient instance."""
    connection_uri = uri or MONGO_URI
    return MongoClient(connection_uri)


def get_database(client: Optional[MongoClient] = None, db_name: Optional[str] = None) -> Database:
    """Returns the database instance."""
    mongo_client = client or get_mongo_client()
    target_db = db_name or DB_NAME
    return mongo_client[target_db]


def init_db(client: Optional[MongoClient] = None, db_name: Optional[str] = None) -> Database:
    """
    Ensures required collections exist and creates necessary indexes:
    - orders_raw: indexed by run_id, ingested_at
    - orders_validated: UNIQUE index on order_id (business key), indexed by customer_id, order_date
    - orders_quarantine: indexed by run_id, error_code
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

    # 3. orders_quarantine indexes
    quarantine_col = db[QUARANTINE_COLLECTION]
    quarantine_col.create_index([("run_id", ASCENDING)], name="idx_quarantine_run_id")
    quarantine_col.create_index([("error_code", ASCENDING)], name="idx_quarantine_error_code")

    return db
