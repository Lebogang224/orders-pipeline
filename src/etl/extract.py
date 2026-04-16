"""
Extract layer — reads raw files into DataFrames.

Rules:
- Read ALL columns as str/object first (dtype=str).
  This ensures bad values are preserved exactly for quarantine logging.
  Casting happens in transform.py, not here.
- Log rows_read and duration for every file.
"""
import time
from pathlib import Path

import pandas as pd

from src.config import Config
from src.logger import get_logger

log = get_logger(__name__)


def extract_customers(cfg: Config) -> pd.DataFrame:
    """Read customers.csv → raw DataFrame (all columns as str)."""
    return _read_csv(cfg.files.customers, "customers")


def extract_orders(cfg: Config) -> pd.DataFrame:
    """Read orders.jsonl → raw DataFrame."""
    path = cfg.files.orders
    t0 = time.perf_counter()
    log.info(f"step=extract_orders file={path}")

    df = pd.read_json(path, lines=True, dtype=str)

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(f"step=extract_orders rows_read={len(df)} duration_ms={elapsed}")
    return df


def extract_order_items(cfg: Config) -> pd.DataFrame:
    """Read order_items.csv → raw DataFrame (all columns as str)."""
    return _read_csv(cfg.files.order_items, "order_items")


def _read_csv(path: Path, label: str) -> pd.DataFrame:
    t0 = time.perf_counter()
    log.info(f"step=extract_{label} file={path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(f"step=extract_{label} rows_read={len(df)} duration_ms={elapsed}")
    return df
