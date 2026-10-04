"""
Practical business queries module for MongoDB orders_validated.
Implements 5 core operational queries with execution and explain('executionStats') support.
"""
from typing import Any, Dict, List, Optional
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


def query_customer_order_history(
    db: Database,
    customer_id: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Query 1: Retrieve all orders for a given customer, sorted chronologically descending.
    Target Index: idx_compound_customer_order_date
    """
    col = db[VALIDATED_COLLECTION]
    cursor = col.find({"customer_id": customer_id}).sort("order_date", DESCENDING).limit(limit)
    return list(cursor)


def query_orders_in_date_range(
    db: Database,
    start_date: str = "2024-01-01T00:00:00Z",
    end_date: str = "2024-12-31T23:59:59Z",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Query 2: Find orders placed within a specific date range.
    Target Index: idx_val_order_date
    """
    col = db[VALIDATED_COLLECTION]
    cursor = (
        col.find({"order_date": {"$gte": start_date, "$lte": end_date}})
        .sort("order_date", ASCENDING)
        .limit(limit)
    )
    return list(cursor)


def query_high_value_orders(
    db: Database,
    min_amount: float = 50.0,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Query 3: Retrieve high-value orders above a specified threshold, sorted by amount descending.
    Target Index: idx_total_amount_desc
    """
    col = db[VALIDATED_COLLECTION]
    cursor = (
        col.find({"total_amount": {"$gte": min_amount}})
        .sort("total_amount", DESCENDING)
        .limit(limit)
    )
    return list(cursor)


def query_customer_orders_in_window(
    db: Database,
    customer_id: str,
    start_date: str = "2023-01-01T00:00:00Z",
    end_date: str = "2025-12-31T23:59:59Z",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Query 4: Retrieve orders for a specific customer within a bounded time interval.
    Target Index: idx_compound_customer_order_date (serves both equality on customer_id and range on order_date)
    """
    col = db[VALIDATED_COLLECTION]
    cursor = (
        col.find(
            {
                "customer_id": customer_id,
                "order_date": {"$gte": start_date, "$lte": end_date},
            }
        )
        .sort("order_date", DESCENDING)
        .limit(limit)
    )
    return list(cursor)


def query_orders_containing_product(
    db: Database,
    item_id: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Query 5: Retrieve orders containing a specific product item_id within the items array.
    Target Index: idx_items_item_id (Multikey index)
    """
    col = db[VALIDATED_COLLECTION]
    cursor = col.find({"items.item_id": item_id}).limit(limit)
    return list(cursor)


# Query Registry with metadata
AVAILABLE_QUERIES = {
    "customer_orders": {
        "function": query_customer_order_history,
        "description": "Orders for a customer sorted by date descending",
        "default_params": {"customer_id": "CUST-001", "limit": 50},
        "target_index": "idx_compound_customer_order_date",
    },
    "date_range_orders": {
        "function": query_orders_in_date_range,
        "description": "Orders within a specified date window",
        "default_params": {"start_date": "2023-01-01T00:00:00Z", "end_date": "2025-12-31T23:59:59Z", "limit": 100},
        "target_index": "idx_val_order_date",
    },
    "high_value_orders": {
        "function": query_high_value_orders,
        "description": "Orders with total amount exceeding threshold sorted descending",
        "default_params": {"min_amount": 30.0, "limit": 50},
        "target_index": "idx_total_amount_desc",
    },
    "customer_date_window": {
        "function": query_customer_orders_in_window,
        "description": "Orders for a specific customer within a date window",
        "default_params": {"customer_id": "CUST-002", "start_date": "2023-01-01T00:00:00Z", "end_date": "2025-12-31T23:59:59Z", "limit": 50},
        "target_index": "idx_compound_customer_order_date",
    },
    "orders_by_product": {
        "function": query_orders_containing_product,
        "description": "Orders containing a specific product SKU / item_id in items array",
        "default_params": {"item_id": "ITM-1", "limit": 50},
        "target_index": "idx_items_item_id",
    },
}


def explain_query_execution(
    db: Database,
    query_name: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Executes explain('executionStats') on the given query.
    Returns parsed metrics: executionTimeMillis, totalDocsExamined, totalKeysExamined, stage, indexName.
    """
    if query_name not in AVAILABLE_QUERIES:
        raise ValueError(f"Unknown query: {query_name}. Available: {list(AVAILABLE_QUERIES.keys())}")

    q_info = AVAILABLE_QUERIES[query_name]
    merged_params = dict(q_info["default_params"])
    if params:
        merged_params.update(params)

    col = db[VALIDATED_COLLECTION]

    # Build the cursor corresponding to the query
    if query_name == "customer_orders":
        cursor = col.find({"customer_id": merged_params["customer_id"]}).sort("order_date", DESCENDING)
    elif query_name == "date_range_orders":
        cursor = col.find({"order_date": {"$gte": merged_params["start_date"], "$lte": merged_params["end_date"]}}).sort("order_date", ASCENDING)
    elif query_name == "high_value_orders":
        cursor = col.find({"total_amount": {"$gte": float(merged_params["min_amount"])}}).sort("total_amount", DESCENDING)
    elif query_name == "customer_date_window":
        cursor = col.find({
            "customer_id": merged_params["customer_id"],
            "order_date": {"$gte": merged_params["start_date"], "$lte": merged_params["end_date"]}
        }).sort("order_date", DESCENDING)
    elif query_name == "orders_by_product":
        cursor = col.find({"items.item_id": merged_params["item_id"]})
    else:
        raise ValueError(f"Unmapped cursor for: {query_name}")

    if "limit" in merged_params:
        cursor = cursor.limit(int(merged_params["limit"]))

    # Execute explain with executionStats
    explain_raw = cursor.explain()["executionStats"]

    # Extract execution stage details recursively
    exec_stages = explain_raw.get("executionStages", {})
    curr = exec_stages
    stages_chain = []
    index_name = None

    while curr and isinstance(curr, dict):
        s = curr.get("stage")
        if s:
            stages_chain.append(s)
        if curr.get("indexName"):
            index_name = curr.get("indexName")
        curr = curr.get("inputStage")

    stage_display = " -> ".join(stages_chain) if stages_chain else "UNKNOWN"

    return {
        "query_name": query_name,
        "target_index": q_info["target_index"],
        "stage": stage_display,
        "index_name": index_name or "None (Collection Scan)",
        "execution_time_ms": explain_raw.get("executionTimeMillis", 0),
        "docs_examined": explain_raw.get("totalDocsExamined", 0),
        "keys_examined": explain_raw.get("totalKeysExamined", 0),
        "n_returned": explain_raw.get("nReturned", 0),
        "params_used": merged_params,
    }
