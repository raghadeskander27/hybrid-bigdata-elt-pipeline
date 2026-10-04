# Hybrid Big Data ELT Pipeline & Analytics Platform (Phase 1 & Phase 2)
## متطلبات المشروع النهائي - Big Data Final Project

An enterprise-grade, idempotent ELT data pipeline and analytical platform engineered with **Python Batch**, **Apache Spark (PySpark)**, **MongoDB**, and **FastAPI**.

---

## 1. System Architecture & Capabilities

```mermaid
flowchart TD
    subgraph Ingestion Layer [Phase 1: Hybrid ELT Ingestion]
        CSV[Input CSV] --> Router[File Router: 200 MB Threshold]
        Router -->|<= 200 MB| Batch[Python Batch Loader]
        Router -->|> 200 MB| Spark[PySpark Loader]
        Batch --> Raw[(MongoDB: orders_raw)]
        Spark --> Raw
        Raw --> Quality[Quality & Validation Engine]
        Quality -->|Valid & Cleaned| Val[(MongoDB: orders_validated)]
        Quality -->|Flagged| Quar[(MongoDB: orders_quarantine)]
    end

    subgraph Analytics Layer [Phase 2: Queries, Indexes & Reports]
        Val --> Q[5 Practical Queries]
        Val --> IDX[Compound & Specialized Indexes]
        Val --> AGG[5 Aggregation Reports]
    end

    subgraph Materialized Views & Automation [Phase 2: Views & Scheduler]
        Val -.Incremental Delta.-> MV[Incremental Materialized Views]
        MV --> DSales[(daily_sales_summary)]
        MV --> TProd[(top_products_summary)]
        Sched[APScheduler Engine] --> MV
        Sched --> JLogs[(MongoDB: job_logs)]
    end

    subgraph Unified Evaluation API [Phase 2: FastAPI Service]
        API[FastAPI Service /docs] --> Ingestion Layer
        API --> Analytics Layer
        API --> Materialized Views & Automation
    end
```

### Core Invariants & Guarantees
1. **ELT Design Pattern:** 100% of raw events ingested into `orders_raw` without pre-filtering.
2. **Consistency Invariant:**
   $$\text{run\_raw\_count} = \text{valid\_count} + \text{corrected\_count} + \text{quarantine\_count}$$
3. **Strict Idempotency:** Validated records write exclusively through MongoDB `UpdateOne({"order_id": ...}, {"$set": ...}, upsert=True)` governed by a unique index on `order_id`. Re-running identical datasets yields `inserted_count == 0` and `unchanged_count == total`.
4. **Incremental Materialized Views:** `daily_sales_summary` and `top_products_summary` process only newly validated delta records using watermarks and atomic `$inc` updates, completely eliminating full collection rebuilds.

---

## 2. Directory Layout

```text
midterm-data-pipeline/
├── requirements.txt            # All runtime & test dependencies
├── .env.example                # Sample environment configurations
├── config/
│   ├── __init__.py
│   └── settings.py             # Centralized settings and collection mappings
├── src/
│   ├── __init__.py
│   ├── mongo_setup.py          # MongoDB setup and index configurations
│   ├── quality_rules.py        # Normalization, quality rules, and audit trail engine
│   ├── file_router.py          # Dynamic router (<= 200MB batch vs > 200MB Spark)
│   ├── batch_loader.py         # Streaming csv.DictReader loader
│   ├── spark_loader.py         # PySpark DataFrame API loader with parallel partitions
│   ├── elt_pipeline.py         # Pipeline orchestrator & invariant verification
│   ├── create_small_sample.py  # Dirty test generator & huge CSV row extractor
│   ├── queries.py              # 5 practical operational queries & explain helper
│   ├── explain_runner.py       # Before/after explain('executionStats') benchmark
│   ├── aggregations.py         # 5 analytical aggregation reports
│   ├── materialized_views.py   # 2 incremental materialized views
│   ├── scheduler.py            # Scheduled background jobs & audit logging
│   └── api.py                  # Unified FastAPI interface (/docs)
├── tests/
│   ├── __init__.py
│   ├── test_cleaning_rules.py  # Phase 1: Quality rules unit tests
│   ├── test_idempotency.py     # Phase 1: Upsert idempotency test suite
│   └── test_phase2.py          # Phase 2: Queries, indexes, views, jobs & API tests
├── data/
│   └── test_dirty_orders.csv   # Edge-case test dataset
├── reports/
│   └── results.json            # Persistent pipeline execution history
├── main.py                     # CLI entrypoint for all pipeline and API actions
└── README.md                   # Complete documentation
```

---

## 3. Installation & Setup

### Prerequisites
- Python 3.10+ (tested on Python 3.12)
- MongoDB running locally on `localhost:27017`
- OpenJDK 17+ (for Apache Spark)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment (Optional)
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Default parameters in `.env`:
```env
MONGO_URI=mongodb://localhost:27017/
DB_NAME=bigdata_elt
FILE_SIZE_THRESHOLD_MB=200.0
API_HOST=127.0.0.1
API_PORT=8000
SCHEDULER_INTERVAL_MINUTES=5
```

---

## 4. Unified FastAPI Interface (واجهة API الموحدة)

Start the unified FastAPI server:
```bash
python main.py --serve-api
```
Interactive Swagger documentation is available at **http://127.0.0.1:8000/docs**.

| HTTP Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Health check verifying MongoDB connectivity and server uptime |
| `POST` | `/ingest` | Triggers the ELT ingestion pipeline using the midterm implementation |
| `POST` | `/indexes` | Builds and verifies all Phase 1 and Phase 2 MongoDB indexes |
| `GET` | `/queries` | Lists all 5 available practical queries with metadata and target indexes |
| `GET` | `/queries/{name}` | Executes a specific query by name (supports `?explain=true` for executionStats) |
| `GET` | `/aggregations` | Lists all 5 analytical aggregation reports |
| `GET` | `/aggregations/{name}` | Executes a specific aggregation report and returns computed results |
| `POST` | `/refresh-mv` | Triggers incremental refresh of materialized views (optional `view_name`) |
| `GET` | `/jobs` | Lists scheduled jobs, next run times, and execution history from `job_logs` |
| `POST` | `/jobs/{name}/run` | Manually triggers a scheduled job on-demand |

---

## 5. Queries, Indexes & Explain Analysis (الاستعلامات والفهارس)

### 1. Five Practical Queries (`src/queries.py`)
1. `customer_orders`: Retrieves orders for a specific customer sorted by date descending.
2. `date_range_orders`: Retrieves all orders placed within a date interval.
3. `high_value_orders`: Filters high-value orders (`total_amount >= threshold`) sorted descending.
4. `customer_date_window`: Bounded query for a customer within a specific promotional/billing window.
5. `orders_by_product`: Finds all orders containing a specific product SKU inside the nested `items` array.

### 2. Indexes Created (`src/mongo_setup.py`)
1. **Compound Index:** `idx_compound_customer_order_date` on `[("customer_id", 1), ("order_date", -1)]`
   - *Rationale:* Satisfies both the equality predicate on `customer_id` and the sort requirement on `order_date`, eliminating in-memory sorting.
2. **Single-Field Range Index:** `idx_total_amount_desc` on `[("total_amount", -1)]`
   - *Rationale:* Enables direct B-Tree traversal for top-amount orders without scanning non-qualifying records.
3. **Multikey Array Index:** `idx_items_item_id` on `[("items.item_id", 1)]`
   - *Rationale:* Indexes every element inside the `items` array, eliminating full collection unnesting scans.

### 3. Explain Benchmark Comparison (`explain("executionStats")`)
Execute the benchmark:
```bash
python main.py --run-explain
```

| Query | Target Index | BEFORE Index (Stage) | AFTER Index (Stage) | Docs Examined | Keys Examined | Performance Impact |
| :--- | :--- | :--- | :--- | :---: | :---: | :--- |
| **Customer Orders** | `idx_compound_customer_order_date` | `SORT -> FETCH -> IXSCAN` | `LIMIT -> FETCH -> IXSCAN` | 1 | 1 | Completely eliminates in-memory `SORT` stage by leveraging index sort order |
| **High Value Orders** | `idx_total_amount_desc` | `SORT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | 6 $\rightarrow$ 5 | 0 $\rightarrow$ 5 | Replaces full collection scan with B-Tree index traversal |
| **Product Search** | `idx_items_item_id` | `LIMIT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | 6 $\rightarrow$ 1 | 0 $\rightarrow$ 1 | Multikey index targets exact array element, dropping docs examined to 1 |

---

## 6. Aggregation Reports (التجميعات - 5 تقارير)

Execute all 5 reports via CLI:
```bash
python main.py --run-aggregations
```
Or via API: `GET /aggregations/{name}`

1. **`top_products`:** Unwinds `$items`, groups by `item_id`, computes `total_quantity`, `total_revenue`, and `order_count`, sorted by revenue descending.
2. **`top_customers`:** Groups by `customer_id`, calculates lifetime `total_spent`, `order_count`, `avg_order_value`, and `last_order_date`.
3. **`sales_by_period`:** Groups orders by day (`$substr: ["$order_date", 0, 10]`), computing daily sales and volume trends.
4. **`sales_by_operator`:** Evaluates the 2-digit prefix of normalized Yemeni phones (`77`, `78`, `73`, `71`, `70`), breaking down revenue and market share by carrier (Yemen Mobile, YOU, Sabafon, Y-Telecom).
5. **`order_value_distribution`:** Buckets order amounts into price tiers (`<25`, `25-50`, `50-100`, `100+ YER`).

---

## 7. Materialized Views (العروض المادية - التحديث التزايدي)

### Two Materialized Views Implemented:
1. `daily_sales_summary`: Precomputed daily aggregate (`day`, `total_sales`, `order_count`, `last_updated`).
2. `top_products_summary`: Precomputed product aggregate (`item_id`, `item_name`, `total_quantity`, `total_revenue`, `last_updated`).

### Incremental Update Mechanism (آلية التحديث التزايدي):
- The pipeline tracks the latest processed `order_date` in `view_metadata` collection as a **watermark**.
- When refreshed, the engine queries **only new delta orders** (`{"order_date": {"$gt": watermark}}`).
- Instead of rebuilding collections, delta aggregates are applied atomically using **MongoDB `$inc` upserts**:
  ```python
  UpdateOne(
      {"day": day},
      {"$inc": {"total_sales": delta_sales, "order_count": delta_count}, "$set": {"last_updated": now}},
      upsert=True
  )
  ```
- If no new orders arrived since the last watermark, the view returns `status: "UP_TO_DATE"` with `0` processed records.

Trigger refresh via CLI:
```bash
python main.py --refresh-mv
```
Or via API: `POST /refresh-mv`

---

## 8. Scheduled Jobs & Audit Logging (المهام المجدولة)

### Two Automated Background Jobs:
1. `refresh_materialized_views`: Automatically updates `daily_sales_summary` and `top_products_summary` incrementally.
2. `daily_executive_report`: Computes pipeline KPI summary (gross revenue, average basket size, quarantine rate, top product).

### Audit Trail Logging (`job_logs` collection):
Every execution (whether triggered on schedule or manually) records a structured audit log in MongoDB:
```json
{
  "job_name": "daily_executive_report",
  "trigger_type": "MANUAL",
  "start_time": "2026-10-04T12:14:32.526265+00:00",
  "end_time": "2026-10-04T12:14:32.564118+00:00",
  "duration_seconds": 0.038,
  "status": "SUCCESS",
  "error_message": null,
  "details": {
    "total_raw_ingested": 17,
    "total_orders_validated": 6,
    "total_orders_quarantined": 11,
    "quarantine_rate_percent": 64.71,
    "gross_revenue": 8750.0,
    "average_order_value": 1458.33
  }
}
```

Trigger manually via CLI:
```bash
python main.py --run-job refresh_materialized_views
python main.py --run-job daily_executive_report
```
Or via API: `POST /jobs/{name}/run`

---

## 9. Running Automated Tests

Run the complete test suite:
```bash
pytest tests/ -v
```
**Test Suite Coverage (32 Tests Passing):**
- 14 tests in `test_cleaning_rules.py`: Yemeni phone normalization, customer_id, items parsing, date boundaries (2000-2030), price sanitation, and audit trail logging.
- 1 test in `test_idempotency.py`: Re-running identical batch yields `inserted_count == 0`, `unchanged_count == total`, and 0 duplicate IDs.
- 17 tests in `test_phase2.py`: All 5 queries, compound indexes, explain comparison runner, all 5 aggregations, incremental materialized view updates, manual job execution, and all 10 FastAPI HTTP endpoints.

---

## 10. Evaluation Guide (دليل التقييم السريع)

For examiners and evaluators, execute the following commands in order:

```bash
# 1. Run full test suite (All 32 tests should pass)
pytest tests/ -v

# 2. Run ELT Ingestion Pipeline (Phase 1)
python main.py --file data/test_dirty_orders.csv

# 3. Re-run Pipeline to verify Idempotency (inserted_count == 0)
python main.py --file data/test_dirty_orders.csv

# 4. Run Explain Benchmark (Before vs After Indexes)
python main.py --run-explain

# 5. Run 5 Analytical Aggregation Reports
python main.py --run-aggregations

# 6. Trigger Incremental Materialized Views Refresh
python main.py --refresh-mv

# 7. Trigger Scheduled Job Manually and view log
python main.py --run-job daily_executive_report

# 8. Start Unified FastAPI Server and visit Swagger docs
python main.py --serve-api
# Open browser at: http://127.0.0.1:8000/docs
```
