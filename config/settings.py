"""
Pipeline configuration settings and environment defaults.
Supports loading configurations from .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env if present
load_dotenv(BASE_DIR / ".env")

# MongoDB Settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "bigdata_elt")

# MongoDB Collection Names - Phase 1 Core
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# MongoDB Collection Names - Phase 2 Extensions
MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"
JOB_LOGS_COLLECTION = "job_logs"
VIEW_METADATA_COLLECTION = "view_metadata"

# File Routing Threshold (in Megabytes)
FILE_SIZE_THRESHOLD_MB = float(os.getenv("FILE_SIZE_THRESHOLD_MB", "200.0"))

# Batch Processing Chunk Size
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "1000"))

# Data Quality Rule Parameters
MIN_VALID_YEAR = 2000
MAX_VALID_YEAR = 2030
YEMENI_PHONE_VALID_PREFIXES = ("77", "78", "73", "71", "70")

# Report & Data Output Directories
REPORTS_DIR = BASE_DIR / "reports"
RESULTS_FILE = REPORTS_DIR / "results.json"
DATA_DIR = BASE_DIR / "data"
DEFAULT_DIRTY_DATASET_PATH = DATA_DIR / "test_dirty_orders.csv"

# API & Scheduler Settings
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
SCHEDULER_INTERVAL_MINUTES = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "5"))

# Ensure runtime directories exist
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
