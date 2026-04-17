"""
Load layer — bulk-load DataFrames into PostgreSQL using psycopg v3 COPY.

Design decision: COPY vs batched INSERT
  - COPY is 10-100x faster (single round-trip vs N round-trips)
  - The brief explicitly says: "Prefer client-side COPY"
  - We pre-validate in pandas so the COPY batch is already clean
  - If a constraint violation still fires (defence-in-depth), the
    transaction rolls back and the error is logged with full context

Quarantine tables are loaded in the same transaction as clean tables
so the entire run is atomic: either everything loads or nothing does.
"""
import io
import time
from typing import Optional

import pandas as pd
import psycopg
from psycopg import sql

from src.config import Config
from src.db.connection import get_connection
from src.logger import get_logger

log = get_logger(__name__)


def load_all(
    cfg: Config,
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    q_customers: pd.DataFrame,
    q_orders: pd.DataFrame,
    q_order_items: pd.DataFrame,
) -> dict:
    """
    Load all six DataFrames in a single transaction.
    Returns a dict of {table: rows_loaded}.
    """
    t0 = time.perf_counter()
    log.info("step=load_all starting transaction")

    results = {}

    with get_connection(cfg) as conn:
        results["customers"]           = _copy_df(conn, "customers",           customers,   _CUSTOMER_COLS)
        results["orders"]              = _copy_df(conn, "orders",              orders,      _ORDER_COLS)
        results["order_items"]         = _copy_df(conn, "order_items",         order_items, _ITEM_COLS)
        results["quarantine_customers"]  = _copy_df(conn, "quarantine_customers",  q_customers,  _Q_CUSTOMER_COLS)
        results["quarantine_orders"]     = _copy_df(conn, "quarantine_orders",     q_orders,     _Q_ORDER_COLS)
        results["quarantine_order_items"]= _copy_df(conn, "quarantine_order_items",q_order_items,_Q_ITEM_COLS)

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(f"step=load_all status=ok duration_ms={elapsed} rows={results}")
    return results


def truncate_all(cfg: Config) -> None:
    """Truncate all primary + quarantine tables (dev/test utility)."""
    tables = [
        "order_items", "orders", "customers",
        "quarantine_order_items", "quarantine_orders", "quarantine_customers",
    ]
    with get_connection(cfg) as conn:
        for table in tables:
            conn.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                sql.Identifier(table)
            ))
    log.info(f"step=truncate tables={tables}")


# ── Column lists ──────────────────────────────────────────────────────────────
_CUSTOMER_COLS   = ["customer_id", "email", "full_name", "signup_date", "country_code", "is_active"]
_ORDER_COLS      = ["order_id", "customer_id", "order_ts", "status", "total_amount", "currency"]
_ITEM_COLS       = ["order_id", "line_no", "sku", "quantity", "unit_price", "category"]

_Q_CUSTOMER_COLS = _CUSTOMER_COLS + ["quarantine_reason", "source_file"]
_Q_ORDER_COLS    = _ORDER_COLS    + ["quarantine_reason", "source_file"]
_Q_ITEM_COLS     = _ITEM_COLS     + ["quarantine_reason", "source_file"]


def _copy_df(
    conn: psycopg.Connection,
    table: str,
    df: pd.DataFrame,
    columns: list[str],
) -> int:
    """
    COPY df into table using psycopg v3 cursor.copy().
    Returns number of rows loaded. Skips gracefully if df is empty.
    """
    if df is None or df.empty:
        log.debug(f"  load table={table} rows=0 (empty)")
        return 0

    # Select + reorder columns; fill missing optional cols with None
    out = pd.DataFrame(index=df.index)
    for col in columns:
        out[col] = df[col] if col in df.columns else None

    # Replace pandas NA/NaT/NaN with None so psycopg sends SQL NULL
    out = out.where(pd.notnull(out), other=None)
    # Also catch string "nan"/"NaT"/"None" that pandas sometimes produces
    out = out.replace({"nan": None, "NaT": None, "None": None, "<NA>": None})

    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT TEXT)").format(
        sql.Identifier(table), col_sql
    )

    t0 = time.perf_counter()
    rows_loaded = 0

    import math

    def _clean_row(row):
        """Convert float NaN and string 'nan' to None for SQL NULL."""
        result = []
        for v in row:
            if v is None:
                result.append(None)
            elif isinstance(v, float) and math.isnan(v):
                result.append(None)
            elif isinstance(v, str) and v.lower() in ("nan", "nat", "none", "<na>"):
                result.append(None)
            else:
                result.append(v)
        return tuple(result)

    with conn.cursor().copy(copy_sql) as cp:
        for row in out.itertuples(index=False, name=None):
            cp.write_row(_clean_row(row))
            rows_loaded += 1

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(f"  load table={table} rows={rows_loaded} duration_ms={elapsed}")
    return rows_loaded
