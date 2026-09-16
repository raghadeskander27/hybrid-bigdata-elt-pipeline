"""
Python batch ingestion loader using streaming csv.DictReader and MongoDB insert_many.
"""
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
from pymongo.database import Database

from config.settings import BATCH_CHUNK_SIZE, RAW_COLLECTION
from src.mongo_setup import get_database


def load_raw_batch(
    file_path: Union[str, Path],
    run_id: str,
    db: Optional[Database] = None,
    chunk_size: int = BATCH_CHUNK_SIZE,
) -> int:
    """
    Streams CSV file using DictReader and inserts all raw records in batches into MongoDB orders_raw.
    ELT Principle: Ingests 100% of rows without dropping or pre-filtering.

    Returns:
        Total number of raw records ingested.
    """
    database = db if db is not None else get_database()
    raw_collection = database[RAW_COLLECTION]
    file = Path(file_path)

    total_ingested = 0
    batch = []

    # Open with utf-8-sig to safely handle potential BOM in CSV files
    with open(file, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_doc = {
                "run_id": run_id,
                "raw_record": dict(row),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            batch.append(raw_doc)

            if len(batch) >= chunk_size:
                raw_collection.insert_many(batch, ordered=False)
                total_ingested += len(batch)
                batch = []

        if batch:
            raw_collection.insert_many(batch, ordered=False)
            total_ingested += len(batch)
            batch = []

    return total_ingested
