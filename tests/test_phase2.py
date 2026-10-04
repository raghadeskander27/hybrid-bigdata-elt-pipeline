"""
Comprehensive test suite for Big Data Phase 2 Final Project requirements:
1. Queries and Indexes (Compound, Range, Multikey) & Explain analysis
2. Aggregation Reports (5 reports)
3. Materialized Views (daily_sales_summary, top_products_summary) with Incremental Update mechanics
4. Scheduled Jobs & Audit Logging in job_logs
5. Unified FastAPI Endpoints (/health, /ingest, /indexes, /queries, /aggregations, /refresh-mv, /jobs)
"""
import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from config.settings import (
    DB_NAME,
    VALIDATED_COLLECTION,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    JOB_LOGS_COLLECTION,
    VIEW_METADATA_COLLECTION,
)
from src.mongo_setup import (
    get_mongo_client,
    get_database,
    init_db,
    create_phase2_indexes,
    drop_phase2_indexes,
    PHASE2_INDEXES,
)
from src.queries import (
    AVAILABLE_QUERIES,
    query_customer_order_history,
    query_orders_in_date_range,
    query_high_value_orders,
    query_customer_orders_in_window,
    query_orders_containing_product,
    explain_query_execution,
)
from src.explain_runner import run_explain_comparison
from src.aggregations import (
    AVAILABLE_AGGREGATIONS,
    report_top_products,
    report_top_customers,
    report_sales_by_period,
    report_sales_by_operator,
    report_order_value_distribution,
    run_aggregation,
)
from src.materialized_views import (
    refresh_daily_sales_summary,
    refresh_top_products_summary,
    refresh_all_materialized_views,
    get_materialized_view_data,
)
from src.scheduler import (
    REGISTERED_JOBS,
    run_job_manually,
    get_scheduler_manager,
)
from src.api import app

PHASE2_TEST_DB = "bigdata_elt_phase2_test"


@pytest.fixture(scope="module")
def setup_phase2_db():
    """Initializes a clean MongoDB test database populated with validated sample records."""
    client = get_mongo_client()
    db = client[PHASE2_TEST_DB]
    client.drop_database(PHASE2_TEST_DB)
    init_db(client, db_name=PHASE2_TEST_DB)

    # Insert representative validated test orders
    sample_orders = [
        {
            "order_id": "ORD-T1",
            "customer_id": "CUST-A",
            "phone": "771234567",
            "items": [
                {"item_id": "SKU-01", "name": "Mechanical Keyboard", "qty": 2, "price": 50.0},
                {"item_id": "SKU-02", "name": "Gaming Mouse", "qty": 1, "price": 25.0},
            ],
            "order_date": "2024-03-01T10:00:00Z",
            "total_amount": 125.0,
            "corrections": [],
            "run_id": "test_run_1",
            "validated_at": "2024-03-01T10:00:00Z",
        },
        {
            "order_id": "ORD-T2",
            "customer_id": "CUST-A",
            "phone": "771234567",
            "items": [
                {"item_id": "SKU-01", "name": "Mechanical Keyboard", "qty": 1, "price": 50.0},
            ],
            "order_date": "2024-03-05T14:30:00Z",
            "total_amount": 50.0,
            "corrections": [],
            "run_id": "test_run_1",
            "validated_at": "2024-03-05T14:30:00Z",
        },
        {
            "order_id": "ORD-T3",
            "customer_id": "CUST-B",
            "phone": "731234568",
            "items": [
                {"item_id": "SKU-03", "name": "USB-C Cable", "qty": 3, "price": 10.0},
            ],
            "order_date": "2024-03-05T16:00:00Z",
            "total_amount": 30.0,
            "corrections": [],
            "run_id": "test_run_1",
            "validated_at": "2024-03-05T16:00:00Z",
        },
        {
            "order_id": "ORD-T4",
            "customer_id": "CUST-C",
            "phone": "711234569",
            "items": [
                {"item_id": "SKU-02", "name": "Gaming Mouse", "qty": 2, "price": 25.0},
            ],
            "order_date": "2024-03-10T09:15:00Z",
            "total_amount": 50.0,
            "corrections": [],
            "run_id": "test_run_1",
            "validated_at": "2024-03-10T09:15:00Z",
        },
        {
            "order_id": "ORD-T5",
            "customer_id": "CUST-D",
            "phone": "701234560",
            "items": [
                {"item_id": "SKU-04", "name": "Laptop Stand", "qty": 1, "price": 15.0},
            ],
            "order_date": "2024-03-15T11:00:00Z",
            "total_amount": 15.0,
            "corrections": [],
            "run_id": "test_run_1",
            "validated_at": "2024-03-15T11:00:00Z",
        },
    ]
    db[VALIDATED_COLLECTION].insert_many(sample_orders)

    yield db
    client.drop_database(PHASE2_TEST_DB)
    client.close()


# 1. Tests for Queries and Indexes
class TestQueriesAndIndexes:

    def test_all_five_queries_execute(self, setup_phase2_db):
        db = setup_phase2_db

        # Query 1: Customer orders sorted by date desc
        q1 = query_customer_order_history(db, "CUST-A")
        assert len(q1) == 2
        assert q1[0]["order_date"] > q1[1]["order_date"]

        # Query 2: Date range orders
        q2 = query_orders_in_date_range(db, "2024-03-01T00:00:00Z", "2024-03-06T00:00:00Z")
        assert len(q2) == 3

        # Query 3: High-value orders
        q3 = query_high_value_orders(db, min_amount=50.0)
        assert len(q3) == 3
        assert all(o["total_amount"] >= 50.0 for o in q3)

        # Query 4: Customer in window
        q4 = query_customer_orders_in_window(db, "CUST-A", "2024-03-01T00:00:00Z", "2024-03-02T00:00:00Z")
        assert len(q4) == 1
        assert q4[0]["order_id"] == "ORD-T1"

        # Query 5: Product in items
        q5 = query_orders_containing_product(db, "SKU-01")
        assert len(q5) == 2

    def test_explain_execution_stats(self, setup_phase2_db):
        db = setup_phase2_db
        stats = explain_query_execution(db, "customer_orders", {"customer_id": "CUST-A"})
        assert "stage" in stats
        assert "execution_time_ms" in stats
        assert "docs_examined" in stats

    def test_explain_runner_before_and_after_indexes(self, setup_phase2_db):
        db = setup_phase2_db
        report = run_explain_comparison(db)
        assert report["status"] == "COMPLETED"
        assert len(report["comparisons"]) == 3
        for comp in report["comparisons"]:
            assert "before" in comp
            assert "after" in comp
            assert "rationale" in comp
            # Ensure indexes are restored after test
            assert comp["after"]["stage"] != ""


# 2. Tests for 5 Aggregation Reports
class TestAggregations:

    def test_report_top_products(self, setup_phase2_db):
        db = setup_phase2_db
        results = report_top_products(db)
        assert len(results) > 0
        # Mechanical keyboard has 3 units * 50.0 = 150.0 revenue
        top = results[0]
        assert "item_id" in top
        assert "total_quantity" in top
        assert "total_revenue" in top

    def test_report_top_customers(self, setup_phase2_db):
        db = setup_phase2_db
        results = report_top_customers(db)
        assert len(results) > 0
        assert results[0]["customer_id"] == "CUST-A"
        assert results[0]["total_spent"] == 175.0
        assert results[0]["order_count"] == 2

    def test_report_sales_by_period(self, setup_phase2_db):
        db = setup_phase2_db
        results = report_sales_by_period(db)
        assert len(results) > 0
        assert all("period" in r and "total_sales" in r for r in results)

    def test_report_sales_by_operator(self, setup_phase2_db):
        db = setup_phase2_db
        results = report_sales_by_operator(db)
        operators = {r["operator"] for r in results}
        assert "Yemen Mobile" in operators  # 77
        assert "YOU (MTN)" in operators     # 73
        assert "Sabafon" in operators       # 71

    def test_report_order_value_distribution(self, setup_phase2_db):
        db = setup_phase2_db
        results = report_order_value_distribution(db)
        assert len(results) > 0
        assert any("Under 1,000 YER" in r["tier"] for r in results)

    def test_run_aggregation_dispatcher(self, setup_phase2_db):
        db = setup_phase2_db
        for name in AVAILABLE_AGGREGATIONS:
            res = run_aggregation(db, name)
            assert isinstance(res, list)


# 3. Tests for Incremental Materialized Views
class TestMaterializedViewsIncremental:

    def test_incremental_refresh_flow(self, setup_phase2_db):
        db = setup_phase2_db

        # Step 1: Initial Refresh (Processes all 5 orders)
        res_initial = refresh_all_materialized_views(db, force_full=True)
        assert res_initial["status"] == "COMPLETED"
        assert res_initial["views"][MV_DAILY_SALES]["delta_records_processed"] == 5

        # Verify documents created
        daily_docs = get_materialized_view_data(db, MV_DAILY_SALES)
        assert len(daily_docs) > 0

        product_docs = get_materialized_view_data(db, MV_TOP_PRODUCTS)
        assert len(product_docs) > 0

        # Step 2: Second Refresh with NO new orders -> status UP_TO_DATE (0 processed)
        res_noop = refresh_daily_sales_summary(db)
        assert res_noop["status"] == "UP_TO_DATE"
        assert res_noop["delta_records_processed"] == 0

        # Step 3: Insert NEW Order with a later order_date
        new_order = {
            "order_id": "ORD-T99",
            "customer_id": "CUST-NEW",
            "phone": "779999999",
            "items": [{"item_id": "SKU-01", "name": "Mechanical Keyboard", "qty": 1, "price": 50.0}],
            "order_date": "2024-04-01T12:00:00Z",
            "total_amount": 50.0,
            "corrections": [],
            "run_id": "test_run_2",
            "validated_at": "2024-04-01T12:00:00Z",
        }
        db[VALIDATED_COLLECTION].insert_one(new_order)

        # Step 4: Incremental refresh processes ONLY the 1 new order
        res_incremental_daily = refresh_daily_sales_summary(db)
        assert res_incremental_daily["status"] == "REFRESHED"
        assert res_incremental_daily["delta_records_processed"] == 1

        res_incremental_products = refresh_top_products_summary(db)
        assert res_incremental_products["status"] == "REFRESHED"
        assert res_incremental_products["delta_records_processed"] == 1

        # Check that SKU-01 was atomically incremented from 3 units (150.0) to 4 units (200.0)
        sku1 = db[MV_TOP_PRODUCTS].find_one({"item_id": "SKU-01"})
        assert sku1["total_quantity"] == 4
        assert sku1["total_revenue"] == 200.0


# 4. Tests for Scheduled Jobs and Execution Logging
class TestSchedulerAndJobLogs:

    def test_manual_job_execution_and_logging(self, setup_phase2_db):
        db = setup_phase2_db

        # Test job 1: refresh_materialized_views
        job1_res = run_job_manually("refresh_materialized_views", db=db)
        assert job1_res["status"] == "SUCCESS"
        assert job1_res["trigger_type"] == "MANUAL"
        assert job1_res["duration_seconds"] >= 0

        # Test job 2: daily_executive_report
        job2_res = run_job_manually("daily_executive_report", db=db)
        assert job2_res["status"] == "SUCCESS"
        assert "gross_revenue" in job2_res["details"]
        assert "quarantine_rate_percent" in job2_res["details"]

        # Verify job_logs in MongoDB
        log_col = db[JOB_LOGS_COLLECTION]
        logs = list(log_col.find({}))
        assert len(logs) >= 2
        for l in logs:
            assert "start_time" in l
            assert "end_time" in l
            assert "status" in l
            assert l["status"] == "SUCCESS"


# 5. Tests for Unified FastAPI Endpoints
class TestFastAPIEndpoints:

    client = TestClient(app)

    def test_health_endpoint(self):
        resp = self.client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["mongodb_connected"] is True
        assert data["status"] in ("UP", "HEALTHY")

    def test_indexes_endpoint(self):
        resp = self.client.post("/indexes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "SUCCESS"
        assert len(data["phase2_indexes"]) >= 3

    def test_queries_endpoints(self):
        # List queries
        resp_list = self.client.get("/queries")
        assert resp_list.status_code == 200
        assert resp_list.json()["count"] == 5

        # Execute single query
        resp_q = self.client.get("/queries/customer_orders?customer_id=CUST-001")
        assert resp_q.status_code == 200
        assert "data" in resp_q.json()

        # Execute query with explain=true
        resp_exp = self.client.get("/queries/high_value_orders?explain=true")
        assert resp_exp.status_code == 200
        data = resp_exp.json()
        assert data["mode"] == "EXPLAIN"
        assert "execution_stats" in data

    def test_aggregations_endpoints(self):
        # List aggregations
        resp_list = self.client.get("/aggregations")
        assert resp_list.status_code == 200
        assert resp_list.json()["count"] == 5

        # Execute single aggregation
        resp_agg = self.client.get("/aggregations/top_products")
        assert resp_agg.status_code == 200
        assert resp_agg.json()["report_name"] == "top_products"
        assert isinstance(resp_agg.json()["data"], list)

    def test_refresh_mv_endpoint(self):
        resp = self.client.post("/refresh-mv", json={})
        assert resp.status_code == 200
        assert resp.json()["status"] == "SUCCESS"

    def test_jobs_endpoints(self):
        # List jobs
        resp_list = self.client.get("/jobs")
        assert resp_list.status_code == 200
        assert "scheduler_status" in resp_list.json()

        # Run job manually via POST /jobs/{name}/run
        resp_run = self.client.post("/jobs/daily_executive_report/run")
        assert resp_run.status_code == 200
        data = resp_run.json()
        assert data["status"] == "SUCCESS"
        assert data["execution"]["status"] == "SUCCESS"
