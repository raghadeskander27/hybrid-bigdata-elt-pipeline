"""
MongoDB Aggregation pipelines module.
Implements 5 analytical reports with independent execution methods:
1. top_products: Best-selling products by quantity and revenue
2. top_customers: Top spending customers with lifetime value and frequency
3. sales_by_period: Daily sales, order volume, and average order value trends
4. sales_by_operator: Sales breakdown across Yemeni mobile network carriers
5. order_value_distribution: Stratification of orders across spending tiers
"""
from typing import Any, Dict, List, Optional
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


def report_top_products(db: Database, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Report 1: Top Products
    Unwinds the items array to calculate total quantity sold, gross revenue,
    and order occurrences per product item_id.
    """
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.item_id",
                "item_name": {"$first": "$items.name"},
                "total_quantity": {"$sum": "$items.qty"},
                "total_revenue": {
                    "$sum": {
                        "$multiply": [
                            {"$ifNull": ["$items.qty", 1]},
                            {"$ifNull": ["$items.price", 0.0]},
                        ]
                    }
                },
                "order_count": {"$sum": 1},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "item_id": "$_id",
                "item_name": {"$ifNull": ["$item_name", "Unknown Item"]},
                "total_quantity": "$total_quantity",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": "$order_count",
            }
        },
    ]
    return list(col.aggregate(pipeline))


def report_top_customers(db: Database, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Report 2: Top Customers
    Aggregates lifetime order spend, total transactions, average basket size,
    and recency for each customer.
    """
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {
            "$group": {
                "_id": "$customer_id",
                "total_spent": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "last_order_date": {"$max": "$order_date"},
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "total_spent": {"$round": ["$total_spent", 2]},
                "order_count": "$order_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_order_date": "$last_order_date",
            }
        },
    ]
    return list(col.aggregate(pipeline))


def report_sales_by_period(db: Database) -> List[Dict[str, Any]]:
    """
    Report 3: Sales by Time Period (Daily)
    Extracts date components from ISO order_date, aggregating daily revenue,
    order count, and average order value.
    """
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {
            "$project": {
                "day": {"$substr": ["$order_date", 0, 10]},
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$day",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": "$order_count",
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
            }
        },
    ]
    return list(col.aggregate(pipeline))


def report_sales_by_operator(db: Database) -> List[Dict[str, Any]]:
    """
    Report 4: Sales by Mobile Carrier Operator
    Extracts the 2-digit telecom prefix from normalized Yemeni phone numbers:
    - 77: Yemen Mobile (CDMA/LTE)
    - 78: Yemen Mobile (Volte)
    - 73: YOU / MTN
    - 71: Sabafon
    - 70: Y-Telecom
    """
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {
            "$project": {
                "prefix": {"$substr": ["$phone", 0, 2]},
                "total_amount": 1,
            }
        },
        {
            "$project": {
                "operator": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$prefix", "77"]}, "then": "Yemen Mobile"},
                            {"case": {"$eq": ["$prefix", "78"]}, "then": "Yemen Mobile (4G)"},
                            {"case": {"$eq": ["$prefix", "73"]}, "then": "YOU (MTN)"},
                            {"case": {"$eq": ["$prefix", "71"]}, "then": "Sabafon"},
                            {"case": {"$eq": ["$prefix", "70"]}, "then": "Y-Telecom"},
                        ],
                        "default": "Other / Unknown",
                    }
                },
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$operator",
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "avg_order_value": {"$avg": "$total_amount"},
            }
        },
        {"$sort": {"total_sales": -1}},
        {
            "$project": {
                "_id": 0,
                "operator": "$_id",
                "order_count": "$order_count",
                "total_sales": {"$round": ["$total_sales", 2]},
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
            }
        },
    ]
    return list(col.aggregate(pipeline))


def report_order_value_distribution(db: Database) -> List[Dict[str, Any]]:
    """
    Report 5: Order Value Distribution (Price Tier Stratification)
    Buckets order amounts into distinct spending tiers.
    """
    col = db[VALIDATED_COLLECTION]
    pipeline = [
        {
            "$project": {
                "tier": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lt": ["$total_amount", 25.0]}, "then": "1. Under 25 YER"},
                            {"case": {"$and": [{"$gte": ["$total_amount", 25.0]}, {"$lt": ["$total_amount", 50.0]}]}, "then": "2. 25 - 50 YER"},
                            {"case": {"$and": [{"$gte": ["$total_amount", 50.0]}, {"$lt": ["$total_amount", 100.0]}]}, "then": "3. 50 - 100 YER"},
                        ],
                        "default": "4. 100+ YER (High Value)",
                    }
                },
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$tier",
                "order_count": {"$sum": 1},
                "total_revenue": {"$sum": "$total_amount"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "tier": "$_id",
                "order_count": "$order_count",
                "total_revenue": {"$round": ["$total_revenue", 2]},
            }
        },
    ]
    return list(col.aggregate(pipeline))


# Aggregations Registry
AVAILABLE_AGGREGATIONS = {
    "top_products": {
        "function": report_top_products,
        "description": "Top-selling products ranked by total revenue and quantity sold",
    },
    "top_customers": {
        "function": report_top_customers,
        "description": "Highest-spending customers by total expenditure and order count",
    },
    "sales_by_period": {
        "function": report_sales_by_period,
        "description": "Daily sales totals, order volume, and average order value",
    },
    "sales_by_operator": {
        "function": report_sales_by_operator,
        "description": "Market distribution and revenue breakdown across Yemeni telecom operators",
    },
    "order_value_distribution": {
        "function": report_order_value_distribution,
        "description": "Stratification of orders across price and spending tiers",
    },
}


def run_aggregation(db: Database, report_name: str, **kwargs) -> List[Dict[str, Any]]:
    """Runs a specific aggregation report by name."""
    if report_name not in AVAILABLE_AGGREGATIONS:
        raise ValueError(f"Unknown report '{report_name}'. Available: {list(AVAILABLE_AGGREGATIONS.keys())}")
    func = AVAILABLE_AGGREGATIONS[report_name]["function"]
    return func(db, **kwargs)
