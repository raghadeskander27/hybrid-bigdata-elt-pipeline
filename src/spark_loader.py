import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

from pyspark.sql import SparkSession
from pymongo import MongoClient
from pymongo.database import Database

from config.settings import MONGO_URI, DB_NAME, RAW_COLLECTION


def get_spark_session(app_name: str = "HybridELTPipeline") -> SparkSession:
    """Creates or retrieves a PySpark SparkSession configured for local execution."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def _write_partition_to_mongo(partition, run_id: str, mongo_uri: str, db_name: str, collection_name: str):
    """Worker task: writes partition rows to MongoDB orders_raw collection in chunks."""
    from pymongo import MongoClient
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col = db[collection_name]
    batch = []
    chunk_size = 1000

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in partition:
        raw_dict = row.asDict(recursive=True)
        # Convert all values to string or preserve None to match raw string schema
        stringified = {k: (str(v) if v is not None else None) for k, v in raw_dict.items()}
        doc = {
            "run_id": run_id,
            "raw_record": stringified,
            "ingested_at": now_iso,
        }
        batch.append(doc)
        if len(batch) >= chunk_size:
            col.insert_many(batch, ordered=False)
            batch = []

    if batch:
        col.insert_many(batch, ordered=False)
        batch = []
    client.close()


def load_raw_spark(
    file_path: Union[str, Path],
    run_id: str,
    mongo_uri: str = MONGO_URI,
    db_name: str = DB_NAME,
    spark: Optional[SparkSession] = None,
) -> int:
    """
    Reads CSV via PySpark DataFrame API with strict string schema (inferSchema=False).
    Ingests all raw events into MongoDB orders_raw via parallel partitions.

    Returns:
        Total number of raw records ingested.
    """
    session = spark or get_spark_session()
    resolved_path = str(Path(file_path).resolve())

    # Strictly read with string schema, no inference to ensure 100% fidelity with raw input
    df = (
        session.read.option("header", "true")
        .option("inferSchema", "false")
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(resolved_path)
    )

    total_count = df.count()

    # Parallel write across partitions to MongoDB orders_raw
    df.foreachPartition(
        lambda partition: _write_partition_to_mongo(
            partition,
            run_id=run_id,
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection_name=RAW_COLLECTION,
        )
    )

    return total_count
