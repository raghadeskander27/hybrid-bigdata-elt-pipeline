"""
FastAPI unified execution and evaluation interface.
Exposes Swagger UI at /docs and implements all required endpoints:
- GET  /health
- POST /ingest
- POST /indexes
- GET  /queries
- GET  /queries/{name}
- GET  /aggregations
- GET  /aggregations/{name}
- POST /refresh-mv
- GET  /jobs
- POST /jobs/{name}/run
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config.settings import (
    DB_NAME,
    DEFAULT_DIRTY_DATASET_PATH,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
)
from src.mongo_setup import (
    get_database,
    get_mongo_client,
    init_db,
    create_phase2_indexes,
    PHASE2_INDEXES,
)
from src.create_small_sample import create_dirty_sample_csv
from src.elt_pipeline import run_pipeline
from src.queries import AVAILABLE_QUERIES, explain_query_execution
from src.aggregations import AVAILABLE_AGGREGATIONS, run_aggregation
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager: initializes DB indexes and starts scheduler on startup."""
    try:
        db = get_database()
        init_db()
        scheduler_mgr = get_scheduler_manager()
        scheduler_mgr.start()
    except Exception as e:
        print(f"[WARN] Startup initialization exception: {e}")
    yield
    try:
        scheduler_mgr = get_scheduler_manager()
        scheduler_mgr.stop()
    except Exception:
        pass


app = FastAPI(
    title="Hybrid Big Data ELT Pipeline API",
    description="Unified Execution & Testing Interface for Big Data Phase 2 Final Project",
    version="2.0.0",
    docs_url="/docs",
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request Models
class IngestRequest(BaseModel):
    file_path: Optional[str] = None
    engine: Optional[str] = "auto"
    db_name: Optional[str] = None


class RefreshMVRequest(BaseModel):
    view_name: Optional[str] = None
    force_full: Optional[bool] = False


# 1. Health Endpoint
@app.get("/health", tags=["System"])
def get_health() -> Dict[str, Any]:
    """Health check: verifies MongoDB connectivity and server status."""
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        mongo_ok = True
    except Exception as e:
        mongo_ok = False

    return {
        "status": "UP" if mongo_ok else "DEGRADED",
        "mongodb_connected": mongo_ok,
        "database": DB_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# 2. Ingestion Endpoint
@app.post("/ingest", tags=["Pipeline"])
def trigger_ingest(request: Optional[IngestRequest] = None) -> Dict[str, Any]:
    """
    Triggers the ELT ingestion pipeline using the midterm implementation.
    Accepts optional file_path and engine ('auto', 'batch', 'spark').
    """
    req = request or IngestRequest()
    target_path = Path(req.file_path) if req.file_path else DEFAULT_DIRTY_DATASET_PATH

    if not target_path.exists():
        create_dirty_sample_csv(target_path)

    try:
        metrics = run_pipeline(
            file_path=target_path,
            engine=req.engine or "auto",
            db_name=req.db_name,
        )
        return {
            "status": "SUCCESS",
            "message": "Ingestion completed successfully",
            "metrics": metrics,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline ingestion failed: {str(e)}")


# 3. Indexes Endpoint
@app.post("/indexes", tags=["Database & Indexes"])
def manage_indexes() -> Dict[str, Any]:
    """Creates/recreates all Phase 1 and Phase 2 indexes in MongoDB."""
    try:
        db = get_database()
        init_db()
        created = create_phase2_indexes(db)
        return {
            "status": "SUCCESS",
            "message": "Indexes created / verified successfully",
            "phase2_indexes": [idx["name"] for idx in PHASE2_INDEXES],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create indexes: {str(e)}")


# 4. List Queries Endpoint
@app.get("/queries", tags=["Queries & Indexes"])
def list_queries() -> Dict[str, Any]:
    """Lists all 5 available practical queries with metadata and target indexes."""
    queries_summary = {}
    for name, meta in AVAILABLE_QUERIES.items():
        queries_summary[name] = {
            "description": meta["description"],
            "target_index": meta["target_index"],
            "default_params": meta["default_params"],
        }
    return {
        "status": "SUCCESS",
        "count": len(queries_summary),
        "queries": queries_summary,
    }


# 5. Execute or Explain Query Endpoint
@app.get("/queries/{name}", tags=["Queries & Indexes"])
def execute_query(
    name: str,
    customer_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_amount: Optional[float] = None,
    item_id: Optional[str] = None,
    limit: Optional[int] = 50,
    explain: bool = False,
) -> Dict[str, Any]:
    """
    Executes a specific query by name.
    If explain=true, returns executionStats (stage, docs examined, execution time).
    """
    if name not in AVAILABLE_QUERIES:
        raise HTTPException(status_code=404, detail=f"Query '{name}' not found. Available: {list(AVAILABLE_QUERIES.keys())}")

    db = get_database()
    params: Dict[str, Any] = {}
    if customer_id is not None:
        params["customer_id"] = customer_id
    if start_date is not None:
        params["start_date"] = start_date
    if end_date is not None:
        params["end_date"] = end_date
    if min_amount is not None:
        params["min_amount"] = min_amount
    if item_id is not None:
        params["item_id"] = item_id
    if limit is not None:
        params["limit"] = limit

    if explain:
        try:
            stats = explain_query_execution(db, name, params)
            return {"status": "SUCCESS", "mode": "EXPLAIN", "execution_stats": stats}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Explain failed: {str(e)}")

    q_info = AVAILABLE_QUERIES[name]
    merged_params = dict(q_info["default_params"])
    merged_params.update(params)

    try:
        func = q_info["function"]
        results = func(db, **merged_params)
        # Convert ObjectId to string for JSON serialization
        for r in results:
            if "_id" in r:
                r["_id"] = str(r["_id"])
        return {
            "status": "SUCCESS",
            "query_name": name,
            "target_index": q_info["target_index"],
            "count": len(results),
            "data": results,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


# 6. List Aggregations Endpoint
@app.get("/aggregations", tags=["Aggregations"])
def list_aggregations() -> Dict[str, Any]:
    """Lists all 5 available aggregation reports."""
    summary = {k: v["description"] for k, v in AVAILABLE_AGGREGATIONS.items()}
    return {
        "status": "SUCCESS",
        "count": len(summary),
        "aggregations": summary,
    }


# 7. Execute Aggregation Endpoint
@app.get("/aggregations/{name}", tags=["Aggregations"])
def execute_aggregation(
    name: str,
    limit: Optional[int] = 10,
) -> Dict[str, Any]:
    """Executes a specific aggregation report by name and returns the results."""
    if name not in AVAILABLE_AGGREGATIONS:
        raise HTTPException(status_code=404, detail=f"Aggregation '{name}' not found. Available: {list(AVAILABLE_AGGREGATIONS.keys())}")

    db = get_database()
    kwargs = {}
    if name in ("top_products", "top_customers") and limit:
        kwargs["limit"] = limit

    try:
        results = run_aggregation(db, name, **kwargs)
        for r in results:
            if "_id" in r and not isinstance(r["_id"], str):
                r["_id"] = str(r["_id"])
        return {
            "status": "SUCCESS",
            "report_name": name,
            "count": len(results),
            "data": results,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {str(e)}")


# 8. Refresh Materialized Views Endpoint
@app.post("/refresh-mv", tags=["Materialized Views"])
def refresh_materialized_views_endpoint(request: Optional[RefreshMVRequest] = None) -> Dict[str, Any]:
    """
    Triggers incremental refresh of materialized views without rebuilding from scratch.
    Optionally specify view_name ('daily_sales_summary' or 'top_products_summary').
    """
    req = request or RefreshMVRequest()
    db = get_database()
    force_full = req.force_full or False

    try:
        if req.view_name == MV_DAILY_SALES:
            res = refresh_daily_sales_summary(db, force_full=force_full)
        elif req.view_name == MV_TOP_PRODUCTS:
            res = refresh_top_products_summary(db, force_full=force_full)
        else:
            res = refresh_all_materialized_views(db, force_full=force_full)
        return {
            "status": "SUCCESS",
            "message": "Materialized view(s) refreshed successfully",
            "details": res,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Materialized view refresh failed: {str(e)}")


# 9. List Scheduled Jobs Endpoint
@app.get("/jobs", tags=["Scheduled Jobs"])
def list_jobs() -> Dict[str, Any]:
    """Lists registered scheduled jobs and their latest execution logs."""
    scheduler_mgr = get_scheduler_manager()
    status = scheduler_mgr.get_status()
    return {
        "status": "SUCCESS",
        "scheduler_status": status,
    }


# 10. Run Scheduled Job Manually Endpoint
@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def run_job_endpoint(name: str) -> Dict[str, Any]:
    """Manually triggers a scheduled job on-demand, logging result to job_logs."""
    if name not in REGISTERED_JOBS:
        raise HTTPException(status_code=404, detail=f"Job '{name}' not found. Available: {list(REGISTERED_JOBS.keys())}")

    db = get_database()
    try:
        res = run_job_manually(name, db=db)
        return {
            "status": "SUCCESS",
            "message": f"Job '{name}' triggered manually",
            "execution": res,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job execution failed: {str(e)}")
