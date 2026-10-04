"""
Scheduled Jobs Engine and Runner module.
Implements:
1. refresh_materialized_views: Periodic incremental refresh of Materialized Views
2. daily_executive_report: Periodic analytical summary of sales and pipeline health

Features:
- Dual execution modes: Scheduled automatic background execution and on-demand manual triggers
- Comprehensive audit logging in MongoDB 'job_logs' tracking start_time, end_time, duration, status, and output
"""
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from pymongo.database import Database

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import (
    JOB_LOGS_COLLECTION,
    VALIDATED_COLLECTION,
    RAW_COLLECTION,
    QUARANTINE_COLLECTION,
    SCHEDULER_INTERVAL_MINUTES,
)
from src.mongo_setup import get_database
from src.materialized_views import refresh_all_materialized_views
from src.aggregations import report_top_products


def log_job_execution(
    db: Database,
    job_name: str,
    trigger_type: str,
    start_time: str,
    end_time: str,
    duration: float,
    status: str,
    details: Dict[str, Any],
    error_message: Optional[str] = None,
) -> str:
    """Inserts a structured log document into MongoDB job_logs collection."""
    col = db[JOB_LOGS_COLLECTION]
    doc = {
        "job_name": job_name,
        "trigger_type": trigger_type,  # 'SCHEDULED' or 'MANUAL'
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": round(duration, 3),
        "status": status,  # 'SUCCESS' or 'FAILED'
        "error_message": error_message,
        "details": details,
    }
    res = col.insert_one(doc)
    return str(res.inserted_id)


def execute_refresh_materialized_views(db: Database) -> Dict[str, Any]:
    """Task: Refreshes all materialized views incrementally."""
    return refresh_all_materialized_views(db, force_full=False)


def execute_daily_executive_report(db: Database) -> Dict[str, Any]:
    """Task: Computes a comprehensive operational and executive summary."""
    val_col = db[VALIDATED_COLLECTION]
    raw_col = db[RAW_COLLECTION]
    quar_col = db[QUARANTINE_COLLECTION]

    total_raw = raw_col.count_documents({})
    total_valid = val_col.count_documents({})
    total_quar = quar_col.count_documents({})

    # Gross revenue
    pipeline = [
        {"$group": {"_id": None, "gross_revenue": {"$sum": "$total_amount"}, "avg_order": {"$avg": "$total_amount"}}}
    ]
    totals = list(val_col.aggregate(pipeline))
    gross_rev = round(totals[0]["gross_revenue"], 2) if totals else 0.0
    avg_order = round(totals[0]["avg_order"], 2) if totals else 0.0

    # Top product
    top_prods = report_top_products(db, limit=1)
    leading_product = top_prods[0] if top_prods else None

    quarantine_rate = round((total_quar / total_raw * 100), 2) if total_raw > 0 else 0.0

    return {
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "total_raw_ingested": total_raw,
        "total_orders_validated": total_valid,
        "total_orders_quarantined": total_quar,
        "quarantine_rate_percent": quarantine_rate,
        "gross_revenue": gross_rev,
        "average_order_value": avg_order,
        "top_product": leading_product,
    }


# Job Registry mapping names to callable handlers and schedules
REGISTERED_JOBS: Dict[str, Dict[str, Any]] = {
    "refresh_materialized_views": {
        "handler": execute_refresh_materialized_views,
        "description": "Incrementally updates daily_sales_summary and top_products_summary materialized views",
        "default_interval_minutes": SCHEDULER_INTERVAL_MINUTES,
    },
    "daily_executive_report": {
        "handler": execute_daily_executive_report,
        "description": "Calculates executive KPI snapshot covering revenue, top product, and quarantine health",
        "default_interval_minutes": SCHEDULER_INTERVAL_MINUTES * 2,
    },
}


def run_job_manually(job_name: str, db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Executes a registered job on-demand (Manual trigger), logging execution result to job_logs.
    """
    if job_name not in REGISTERED_JOBS:
        raise ValueError(f"Unknown job '{job_name}'. Available: {list(REGISTERED_JOBS.keys())}")

    target_db = db if db is not None else get_database()
    handler = REGISTERED_JOBS[job_name]["handler"]

    start_dt = datetime.now(timezone.utc)
    t0 = time.time()
    status = "SUCCESS"
    details = {}
    err_msg = None

    try:
        details = handler(target_db)
    except Exception as e:
        status = "FAILED"
        err_msg = str(e)
        details = {"traceback": traceback.format_exc()}

    duration = time.time() - t0
    end_dt = datetime.now(timezone.utc)

    log_id = log_job_execution(
        db=target_db,
        job_name=job_name,
        trigger_type="MANUAL",
        start_time=start_dt.isoformat(),
        end_time=end_dt.isoformat(),
        duration=duration,
        status=status,
        details=details,
        error_message=err_msg,
    )

    return {
        "log_id": log_id,
        "job_name": job_name,
        "trigger_type": "MANUAL",
        "status": status,
        "start_time": start_dt.isoformat(),
        "end_time": end_dt.isoformat(),
        "duration_seconds": round(duration, 3),
        "error_message": err_msg,
        "details": details,
    }


def _run_scheduled_job_wrapper(job_name: str, db_name: Optional[str] = None):
    """Wrapper function invoked by the APScheduler for scheduled triggers."""
    db = get_database(db_name=db_name)
    handler = REGISTERED_JOBS[job_name]["handler"]

    start_dt = datetime.now(timezone.utc)
    t0 = time.time()
    status = "SUCCESS"
    details = {}
    err_msg = None

    try:
        details = handler(db)
    except Exception as e:
        status = "FAILED"
        err_msg = str(e)
        details = {"traceback": traceback.format_exc()}

    duration = time.time() - t0
    end_dt = datetime.now(timezone.utc)

    log_job_execution(
        db=db,
        job_name=job_name,
        trigger_type="SCHEDULED",
        start_time=start_dt.isoformat(),
        end_time=end_dt.isoformat(),
        duration=duration,
        status=status,
        details=details,
        error_message=err_msg,
    )


class JobSchedulerManager:
    """Manages the lifecycle of the background APScheduler."""

    def __init__(self, db_name: Optional[str] = None):
        self.scheduler = BackgroundScheduler()
        self.db_name = db_name
        self._is_running = False

    def start(self):
        """Registers and starts all scheduled jobs in background."""
        if self._is_running:
            return

        for job_name, meta in REGISTERED_JOBS.items():
            interval = meta["default_interval_minutes"]
            self.scheduler.add_job(
                func=_run_scheduled_job_wrapper,
                trigger=IntervalTrigger(minutes=interval),
                args=[job_name, self.db_name],
                id=job_name,
                name=meta["description"],
                replace_existing=True,
            )

        self.scheduler.start()
        self._is_running = True

    def stop(self):
        """Stops the background scheduler."""
        if self._is_running:
            self.scheduler.shutdown(wait=False)
            self._is_running = False

    def get_status(self) -> Dict[str, Any]:
        """Returns the status of all registered jobs and recent log entries."""
        db = get_database(db_name=self.db_name)
        col = db[JOB_LOGS_COLLECTION]

        jobs_status = []
        for name, meta in REGISTERED_JOBS.items():
            # Get latest execution log
            latest_log = col.find_one({"job_name": name}, sort=[("start_time", -1)])
            if latest_log:
                latest_log["_id"] = str(latest_log["_id"])

            job_instance = self.scheduler.get_job(name) if self._is_running else None
            next_run = job_instance.next_run_time.isoformat() if (job_instance and job_instance.next_run_time) else None

            jobs_status.append({
                "job_name": name,
                "description": meta["description"],
                "interval_minutes": meta["default_interval_minutes"],
                "is_active": self._is_running,
                "next_run_time": next_run,
                "latest_execution": latest_log,
            })

        return {
            "scheduler_running": self._is_running,
            "jobs_count": len(jobs_status),
            "jobs": jobs_status,
        }


# Singleton manager instance
_scheduler_manager_instance: Optional[JobSchedulerManager] = None


def get_scheduler_manager(db_name: Optional[str] = None) -> JobSchedulerManager:
    """Returns or creates the global JobSchedulerManager singleton."""
    global _scheduler_manager_instance
    if _scheduler_manager_instance is None:
        _scheduler_manager_instance = JobSchedulerManager(db_name=db_name)
    return _scheduler_manager_instance
