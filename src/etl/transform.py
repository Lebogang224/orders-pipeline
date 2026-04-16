"""
Transform layer — clean and validate each entity.

Each function returns (clean_df, quarantine_df).
All quarantine decisions are documented with explicit reason strings
so they can be surfaced in v_quarantine_summary.

Validation order matters:
  1. Structural / format checks first (bad email, bad date)
  2. Business rules second (dup email, invalid status)
  3. Referential integrity last (unknown FK)

This order ensures we know WHY a row was rejected.
"""
import re
import time
from typing import Tuple

import pandas as pd

from src.config import Config
from src.etl.quarantine import split
from src.logger import get_logger

log = get_logger(__name__)

# Email: must have exactly one @, non-empty parts either side, a dot in domain
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ALLOWED_STATUSES = {"placed", "shipped", "cancelled", "refunded"}
_CUSTOMERS_FILE = "customers.csv"
_ORDERS_FILE    = "orders.jsonl"
_ITEMS_FILE     = "order_items.csv"


# =============================================================================
# CUSTOMERS
# =============================================================================

def transform_customers(
    df: pd.DataFrame,
    cfg: Config,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean customers.csv and return (clean, quarantine).

    Steps:
      1. Normalize email: strip whitespace + lowercase
      2. Reject rows with invalid email format → quarantine (invalid_email_format)
      3. Parse signup_date → date; reject unparseable → quarantine (invalid_signup_date)
      4. Cast is_active → bool; default True if missing
      5. Handle null country_code per config
      6. Resolve duplicate emails: keep earliest signup_date → quarantine losers (duplicate_email)
      7. Cast customer_id → int
    """
    t0 = time.perf_counter()
    log.info(f"step=transform_customers rows_in={len(df)}")

    quarantine_frames = []
    working = df.copy()

    # ── Step 1: Normalize email ───────────────────────────────────────────────
    working["email"] = working["email"].str.strip().str.lower()

    # ── Step 2: Reject invalid email format ──────────────────────────────────
    valid_email_mask = working["email"].str.match(_EMAIL_RE, na=False)
    working, bad = split(working, valid_email_mask, "invalid_email_format", _CUSTOMERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=invalid_email_format rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 3: Parse signup_date ─────────────────────────────────────────────
    working["signup_date"] = pd.to_datetime(
        working["signup_date"], errors="coerce", format="mixed"
    ).dt.date
    valid_date_mask = working["signup_date"].notna()
    working, bad = split(working, valid_date_mask, "invalid_signup_date", _CUSTOMERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=invalid_signup_date rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 4: Cast is_active → bool ─────────────────────────────────────────
    working["is_active"] = (
        working["is_active"]
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False})
        .fillna(True)
        .astype(bool)
    )

    # ── Step 5: country_code ─────────────────────────────────────────────────
    working["country_code"] = working["country_code"].str.strip().str.upper()
    working["country_code"] = working["country_code"].where(
        working["country_code"].notna() & (working["country_code"] != ""), other=None
    )
    if not cfg.etl.allow_null_country_code:
        valid_cc_mask = working["country_code"].notna()
        working, bad = split(working, valid_cc_mask, "missing_country_code", _CUSTOMERS_FILE)
        if not bad.empty:
            log.warning(f"  quarantine reason=missing_country_code rows={len(bad)}")
            quarantine_frames.append(bad)

    # ── Step 6: Deduplicate emails — keep earliest signup_date ───────────────
    # Sort so earliest comes first, then drop_duplicates keeps first occurrence
    working = working.sort_values("signup_date", ascending=True)
    dupes_mask = working.duplicated(subset=["email"], keep="first")
    working, bad = split(working, ~dupes_mask, "duplicate_email", _CUSTOMERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=duplicate_email rows={len(bad)} emails={bad['email'].tolist()}")
        quarantine_frames.append(bad)

    # ── Step 7: Cast customer_id → int ───────────────────────────────────────
    working["customer_id"] = pd.to_numeric(working["customer_id"], errors="coerce").astype("Int64")

    quarantine = (
        pd.concat(quarantine_frames, ignore_index=True)
        if quarantine_frames else pd.DataFrame()
    )

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(
        f"step=transform_customers rows_out={len(working)} "
        f"rows_quarantined={len(quarantine)} duration_ms={elapsed}"
    )
    return working, quarantine


# =============================================================================
# ORDERS
# =============================================================================

def transform_orders(
    df: pd.DataFrame,
    valid_customer_ids: set,
    cfg: Config,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean orders.jsonl and return (clean, quarantine).

    Steps:
      1. Parse order_ts with pd.to_datetime(errors='coerce', utc=True)
         Handles: ISO-8601 with TZ, ISO without TZ, space-separated, slash-delimited
      2. Reject unparseable timestamps → quarantine (invalid_order_ts)
      3. Reject status ∉ ALLOWED_STATUSES → quarantine (invalid_status)
      4. Reject customer_id ∉ valid_customer_ids → quarantine (missing_customer_fk)
      5. Cast total_amount → Decimal-compatible float; reject negative
      6. Cast order_id, customer_id → int
    """
    t0 = time.perf_counter()
    log.info(f"step=transform_orders rows_in={len(df)}")

    quarantine_frames = []
    working = df.copy()

    # ── Step 1 & 2: Parse order_ts → UTC ─────────────────────────────────────
    # pd.to_datetime handles all formats in the sample data:
    # "2024-03-01T08:12:00+02:00", "2024-03-01T09:00:00Z",
    # "2024-03-03 11:30:00", "2024/03/04 12:00:00"
    working["order_ts"] = pd.to_datetime(
        working["order_ts"], errors="coerce", utc=True, format="mixed"
    )
    valid_ts_mask = working["order_ts"].notna()
    working, bad = split(working, valid_ts_mask, "invalid_order_ts", _ORDERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=invalid_order_ts rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 3: Validate status ───────────────────────────────────────────────
    working["status"] = working["status"].str.strip().str.lower()
    valid_status_mask = working["status"].isin(_ALLOWED_STATUSES)
    working, bad = split(working, valid_status_mask, "invalid_status", _ORDERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=invalid_status rows={len(bad)} values={bad['status'].tolist()}")
        quarantine_frames.append(bad)

    # ── Step 4: Validate customer FK ─────────────────────────────────────────
    working["customer_id"] = pd.to_numeric(working["customer_id"], errors="coerce").astype("Int64")
    valid_fk_mask = working["customer_id"].isin(valid_customer_ids)
    working, bad = split(working, valid_fk_mask, "missing_customer_fk", _ORDERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=missing_customer_fk rows={len(bad)} ids={bad['customer_id'].tolist()}")
        quarantine_frames.append(bad)

    # ── Step 5: Cast total_amount ─────────────────────────────────────────────
    working["total_amount"] = pd.to_numeric(working["total_amount"], errors="coerce")
    valid_amount_mask = working["total_amount"].notna() & (working["total_amount"] >= 0)
    working, bad = split(working, valid_amount_mask, "invalid_total_amount", _ORDERS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=invalid_total_amount rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 6: Cast order_id → int ───────────────────────────────────────────
    working["order_id"] = pd.to_numeric(working["order_id"], errors="coerce").astype("Int64")

    # Normalise currency
    working["currency"] = working["currency"].str.strip().str.upper().fillna(cfg.etl.default_currency)

    quarantine = (
        pd.concat(quarantine_frames, ignore_index=True)
        if quarantine_frames else pd.DataFrame()
    )

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(
        f"step=transform_orders rows_out={len(working)} "
        f"rows_quarantined={len(quarantine)} duration_ms={elapsed}"
    )
    return working, quarantine


# =============================================================================
# ORDER ITEMS
# =============================================================================

def transform_order_items(
    df: pd.DataFrame,
    valid_order_ids: set,
    cfg: Config,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean order_items.csv and return (clean, quarantine).

    Steps:
      1. Cast quantity → int; reject non-positive → quarantine (non_positive_quantity)
      2. Cast unit_price → float; reject non-positive → quarantine (non_positive_unit_price)
      3. Reject order_id ∉ valid_order_ids → quarantine (missing_order_fk)
    """
    t0 = time.perf_counter()
    log.info(f"step=transform_order_items rows_in={len(df)}")

    quarantine_frames = []
    working = df.copy()

    # ── Cast numerics ─────────────────────────────────────────────────────────
    working["quantity"]   = pd.to_numeric(working["quantity"],   errors="coerce")
    working["unit_price"] = pd.to_numeric(working["unit_price"], errors="coerce")
    working["order_id"]   = pd.to_numeric(working["order_id"],   errors="coerce").astype("Int64")
    working["line_no"]    = pd.to_numeric(working["line_no"],    errors="coerce").astype("Int64")

    # ── Step 1: Reject non-positive quantity ──────────────────────────────────
    valid_qty_mask = working["quantity"].notna() & (working["quantity"] > 0)
    working, bad = split(working, valid_qty_mask, "non_positive_quantity", _ITEMS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=non_positive_quantity rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 2: Reject non-positive unit_price ────────────────────────────────
    valid_price_mask = working["unit_price"].notna() & (working["unit_price"] > 0)
    working, bad = split(working, valid_price_mask, "non_positive_unit_price", _ITEMS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=non_positive_unit_price rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Step 3: Validate order FK ─────────────────────────────────────────────
    valid_fk_mask = working["order_id"].isin(valid_order_ids)
    working, bad = split(working, valid_fk_mask, "missing_order_fk", _ITEMS_FILE)
    if not bad.empty:
        log.warning(f"  quarantine reason=missing_order_fk rows={len(bad)}")
        quarantine_frames.append(bad)

    # ── Final casts ───────────────────────────────────────────────────────────
    working["quantity"]   = working["quantity"].astype(int)
    working["unit_price"] = working["unit_price"].round(2)

    quarantine = (
        pd.concat(quarantine_frames, ignore_index=True)
        if quarantine_frames else pd.DataFrame()
    )

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(
        f"step=transform_order_items rows_out={len(working)} "
        f"rows_quarantined={len(quarantine)} duration_ms={elapsed}"
    )
    return working, quarantine
