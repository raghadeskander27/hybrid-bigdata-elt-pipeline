"""
Materialized Views management module with incremental update mechanics.
Implements:
1. daily_sales_summary: Daily revenue and order volume aggregates
2. top_products_summary: Cumulative product quantity and revenue aggregates

Incremental Logic:
Tracks watermark in 'view_metadata' (last_processed_order_date).
On refresh, processes only newly validated delta records (order_date > watermark)
and applies atomic $inc updates via MongoDB UpdateOne with upsert=True,
avoiding full collection recomputations.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pymongo import UpdateOne
from pymongo.database import Database

from config.settings import (
    VALIDATED_COLLECTION,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    VIEW_METADATA_COLLECTION,
)
from src.mongo_setup import get_database


def get_view_watermark(db: Database, view_name: str) -> Optional[str]:
    """Retrieves the last processed order_date watermark for a given materialized view."""
    meta_col = db[VIEW_METADATA_COLLECTION]
    doc = meta_col.find_one({"view_name": view_name})
    if doc and "last_processed_order_date" in doc:
        return doc["last_processed_order_date"]
    return None


def update_view_watermark(
    db: Database,
    view_name: str,
    new_watermark: str,
    records_processed: int,
) -> None:
    """Updates the watermark and execution metadata for a materialized view."""
    meta_col = db[VIEW_METADATA_COLLECTION]
    now_iso = datetime.now(timezone.utc).isoformat()
    meta_col.update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_processed_order_date": new_watermark,
                "last_refreshed_at": now_iso,
                "last_delta_count": records_processed,
            },
            "$inc": {"total_processed_orders": records_processed},
        },
        upsert=True,
    )


def refresh_daily_sales_summary(db: Database, force_full: bool = False) -> Dict[str, Any]:
    """
    Incrementally updates the daily_sales_summary materialized view.
    Reads delta records where order_date > watermark, aggregates by day,
    and atomically increments existing day totals via UpdateOne($inc).
    """
    val_col = db[VALIDATED_COLLECTION]
    mv_col = db[MV_DAILY_SALES]
    now_iso = datetime.now(timezone.utc).isoformat()

    watermark = None if force_full else get_view_watermark(db, MV_DAILY_SALES)
    match_query: Dict[str, Any] = {}
    if watermark:
        match_query = {"order_date": {"$gt": watermark}}

    # Check max order_date and count of delta records
    delta_orders = list(val_col.find(match_query, {"order_date": 1}).sort("order_date", 1))
    if not delta_orders:
        return {
            "view_name": MV_DAILY_SALES,
            "status": "UP_TO_DATE",
            "delta_records_processed": 0,
            "watermark": watermark,
            "timestamp": now_iso,
        }

    max_order_date = delta_orders[-1]["order_date"]
    delta_count = len(delta_orders)

    # Aggregation on delta only
    pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "day": {"$substr": ["$order_date", 0, 10]},
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$day",
                "delta_sales": {"$sum": "$total_amount"},
                "delta_orders": {"$sum": 1},
            }
        },
    ]

    aggregates = list(val_col.aggregate(pipeline))
    if force_full:
        mv_col.delete_many({})

    operations = []
    for agg in aggregates:
        day = agg["_id"]
        sales = round(agg["delta_sales"], 2)
        count = agg["delta_orders"]

        op = UpdateOne(
            {"day": day},
            {
                "$inc": {
                    "total_sales": sales,
                    "order_count": count,
                },
                "$set": {
                    "day": day,
                    "last_updated": now_iso,
                },
            },
            upsert=True,
        )
        operations.append(op)

    upserted_count = 0
    modified_count = 0
    if operations:
        result = mv_col.bulk_write(operations, ordered=False)
        upserted_count = result.upserted_count
        modified_count = result.modified_count

    update_view_watermark(db, MV_DAILY_SALES, max_order_date, delta_count)

    return {
        "view_name": MV_DAILY_SALES,
        "status": "REFRESHED",
        "mode": "FULL" if force_full else "INCREMENTAL",
        "delta_records_processed": delta_count,
        "days_affected": len(operations),
        "days_upserted": upserted_count,
        "days_updated": modified_count,
        "new_watermark": max_order_date,
        "timestamp": now_iso,
    }


def refresh_top_products_summary(db: Database, force_full: bool = False) -> Dict[str, Any]:
    """
    Incrementally updates the top_products_summary materialized view.
    Reads delta records where order_date > watermark, unwinds items,
    and atomically increments quantity and revenue per item_id via UpdateOne($inc).
    """
    val_col = db[VALIDATED_COLLECTION]
    mv_col = db[MV_TOP_PRODUCTS]
    now_iso = datetime.now(timezone.utc).isoformat()

    watermark = None if force_full else get_view_watermark(db, MV_TOP_PRODUCTS)
    match_query: Dict[str, Any] = {}
    if watermark:
        match_query = {"order_date": {"$gt": watermark}}

    delta_orders = list(val_col.find(match_query, {"order_date": 1}).sort("order_date", 1))
    if not delta_orders:
        return {
            "view_name": MV_TOP_PRODUCTS,
            "status": "UP_TO_DATE",
            "delta_records_processed": 0,
            "watermark": watermark,
            "timestamp": now_iso,
        }

    max_order_date = delta_orders[-1]["order_date"]
    delta_count = len(delta_orders)

    # Aggregation on delta items only
    pipeline = [
        {"$match": match_query},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.item_id",
                "item_name": {"$first": "$items.name"},
                "delta_quantity": {"$sum": "$items.qty"},
                "delta_revenue": {
                    "$sum": {
                        "$multiply": [
                            {"$ifNull": ["$items.qty", 1]},
                            {"$ifNull": ["$items.price", 0.0]},
                        ]
                    }
                },
            }
        },
    ]

    aggregates = list(val_col.aggregate(pipeline))
    if force_full:
        mv_col.delete_many({})

    operations = []
    for agg in aggregates:
        item_id = agg["_id"]
        item_name = agg.get("item_name") or "Unknown Product"
        qty = agg["delta_quantity"]
        rev = round(agg["delta_revenue"], 2)

        op = UpdateOne(
            {"item_id": item_id},
            {
                "$inc": {
                    "total_quantity": qty,
                    "total_revenue": rev,
                },
                "$set": {
                    "item_id": item_id,
                    "item_name": item_name,
                    "last_updated": now_iso,
                },
            },
            upsert=True,
        )
        operations.append(op)

    upserted_count = 0
    modified_count = 0
    if operations:
        result = mv_col.bulk_write(operations, ordered=False)
        upserted_count = result.upserted_count
        modified_count = result.modified_count

    update_view_watermark(db, MV_TOP_PRODUCTS, max_order_date, delta_count)

    return {
        "view_name": MV_TOP_PRODUCTS,
        "status": "REFRESHED",
        "mode": "FULL" if force_full else "INCREMENTAL",
        "delta_records_processed": delta_count,
        "products_affected": len(operations),
        "products_upserted": upserted_count,
        "products_updated": modified_count,
        "new_watermark": max_order_date,
        "timestamp": now_iso,
    }


def refresh_all_materialized_views(db: Database, force_full: bool = False) -> Dict[str, Any]:
    """Refreshes all materialized views in sequence."""
    res_daily = refresh_daily_sales_summary(db, force_full=force_full)
    res_products = refresh_top_products_summary(db, force_full=force_full)
    return {
        "status": "COMPLETED",
        "views": {
            MV_DAILY_SALES: res_daily,
            MV_TOP_PRODUCTS: res_products,
        },
    }


def get_materialized_view_data(db: Database, view_name: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Fetches data from a materialized view collection."""
    if view_name == MV_DAILY_SALES:
        col = db[MV_DAILY_SALES]
        return list(col.find({}, {"_id": 0}).sort("day", -1).limit(limit))
    elif view_name == MV_TOP_PRODUCTS:
        col = db[MV_TOP_PRODUCTS]
        return list(col.find({}, {"_id": 0}).sort("total_revenue", -1).limit(limit))
    else:
        raise ValueError(f"Unknown materialized view: {view_name}")
