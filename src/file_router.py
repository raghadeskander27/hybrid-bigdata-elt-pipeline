"""
File router module.
Determines whether incoming data files should be processed via Python batch or PySpark,
based on file size threshold.
"""
import os
from pathlib import Path
from typing import Union

from config.settings import FILE_SIZE_THRESHOLD_MB

BYTES_PER_MB = 1024 * 1024


def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """Returns the size of the given file in Megabytes."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    return path.stat().st_size / BYTES_PER_MB


def route_file(file_path: Union[str, Path], threshold_mb: float = FILE_SIZE_THRESHOLD_MB) -> str:
    """
    Routes file based on size:
    - size <= threshold_mb -> 'python_batch'
    - size > threshold_mb  -> 'pyspark'
    """
    size_mb = get_file_size_mb(file_path)
    if size_mb <= threshold_mb:
        return "python_batch"
    return "pyspark"
