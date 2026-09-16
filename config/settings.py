"""
Pipeline configuration settings and environment defaults.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# MongoDB Settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "bigdata_elt")

# MongoDB Collection Names
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

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

# Ensure runtime directories exist
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
