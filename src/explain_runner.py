"""
Explain comparison runner module.
Executes explain('executionStats') for 3 targeted queries BEFORE and AFTER index creation,
documenting index selection rationale and measurable performance gains.
"""
from typing import Any, Dict, List
from pymongo.database import Database

from config.settings import DB_NAME
from src.mongo_setup import get_database, create_phase2_indexes, drop_phase2_indexes
from src.queries import explain_query_execution

BENCHMARK_QUERIES = [
    {
        "query_name": "customer_orders",
        "title": "Query 1: Customer Order History (Compound Index)",
        "params": {"customer_id": "CUST-001", "limit": 50},
        "target_index": "idx_compound_customer_order_date",
        "index_type": "Compound Index (customer_id ASC, order_date DESC)",
        "rationale": (
            "Customers frequently view their past orders sorted by date. A compound index "
            "simultaneously filters by customer_id and returns results pre-sorted by order_date, "
            "eliminating both a full collection scan (COLLSCAN) and an in-memory sort stage (SORT)."
        ),
    },
    {
        "query_name": "high_value_orders",
        "title": "Query 2: High-Value Order Filtering (Range Index)",
        "params": {"min_amount": 30.0, "limit": 50},
        "target_index": "idx_total_amount_desc",
        "index_type": "Single-field Descending Index (total_amount DESC)",
        "rationale": (
            "Financial audits and VIP dashboards filter by high order amounts. A single-field "
            "descending index allows B-Tree range traversal directly from top values without "
            "scanning non-qualifying low-value records."
        ),
    },
    {
        "query_name": "orders_by_product",
        "title": "Query 3: Product Search Inside Items Array (Multikey Index)",
        "params": {"item_id": "ITM-1", "limit": 50},
        "target_index": "idx_items_item_id",
        "index_type": "Multikey Index (items.item_id ASC)",
        "rationale": (
            "Inventory tracking requires querying orders containing a specific product SKU. Because "
            "'items' is an array of subdocuments, a multikey index indexes each array element "
            "individually, preventing expensive document unpacking and collection-wide scans."
        ),
    },
]


def run_explain_comparison(db: Database) -> Dict[str, Any]:
    """
    Benchmarks 3 queries before and after creating indexes.
    1. Drops Phase 2 indexes to measure COLLSCAN baseline.
    2. Runs explain('executionStats') for each query.
    3. Recreates Phase 2 indexes.
    4. Runs explain('executionStats') to measure IXSCAN performance.
    """
    # 1. Drop Phase 2 indexes to capture baseline
    drop_phase2_indexes(db)

    before_stats: List[Dict[str, Any]] = []
    for item in BENCHMARK_QUERIES:
        stat = explain_query_execution(db, item["query_name"], item["params"])
        before_stats.append(stat)

    # 2. Create Phase 2 indexes
    create_phase2_indexes(db)

    # 3. Capture post-index stats
    after_stats: List[Dict[str, Any]] = []
    for item in BENCHMARK_QUERIES:
        stat = explain_query_execution(db, item["query_name"], item["params"])
        after_stats.append(stat)

    # 4. Compile comparison report
    comparisons = []
    for i, meta in enumerate(BENCHMARK_QUERIES):
        b = before_stats[i]
        a = after_stats[i]
        comparisons.append({
            "title": meta["title"],
            "query_name": meta["query_name"],
            "target_index": meta["target_index"],
            "index_type": meta["index_type"],
            "rationale": meta["rationale"],
            "before": {
                "stage": b["stage"],
                "index_used": b["index_name"],
                "docs_examined": b["docs_examined"],
                "keys_examined": b["keys_examined"],
                "execution_time_ms": b["execution_time_ms"],
            },
            "after": {
                "stage": a["stage"],
                "index_used": a["index_name"],
                "docs_examined": a["docs_examined"],
                "keys_examined": a["keys_examined"],
                "execution_time_ms": a["execution_time_ms"],
            },
            "impact_summary": (
                f"Transitioned from {b['stage']} (examined {b['docs_examined']} docs) "
                f"to {a['stage']} using {a['index_name']} (examined {a['docs_examined']} docs, {a['keys_examined']} keys). "
                f"Eliminates unnecessary document scanning and in-memory sorting."
            ),
        })

    return {
        "status": "COMPLETED",
        "benchmark_queries_count": len(comparisons),
        "comparisons": comparisons,
    }


def print_explain_report(report: Dict[str, Any]):
    print("\n" + "=" * 80)
    print("         MONGODB EXPLAIN('executionStats') BEFORE / AFTER COMPARISON         ")
    print("=" * 80)

    for item in report["comparisons"]:
        print(f"\n[QUERY] {item['title']}")
        print(f"  Target Index : {item['target_index']} ({item['index_type']})")
        print(f"  Rationale    : {item['rationale']}")
        print("  " + "-" * 76)
        print(f"  {'Metric':<25} | {'BEFORE Index':<25} | {'AFTER Index':<25}")
        print("  " + "-" * 76)
        print(f"  {'Execution Stage':<25} | {item['before']['stage']:<25} | {item['after']['stage']:<25}")
        print(f"  {'Index Used':<25} | {item['before']['index_used']:<25} | {item['after']['index_used']:<25}")
        print(f"  {'Docs Examined':<25} | {item['before']['docs_examined']:<25} | {item['after']['docs_examined']:<25}")
        print(f"  {'Keys Examined':<25} | {item['before']['keys_examined']:<25} | {item['after']['keys_examined']:<25}")
        print(f"  {'Execution Time (ms)':<25} | {item['before']['execution_time_ms']:<25} | {item['after']['execution_time_ms']:<25}")
        print("  " + "-" * 76)
        print(f"  Impact: {item['impact_summary']}\n")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    db = get_database()
    report = run_explain_comparison(db)
    print_explain_report(report)
