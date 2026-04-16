"""
Pipeline orchestrator — wires Extract → Transform → Load together.

Called by: python main.py run

This is the single entry point for a full ETL run. It:
  1. Extracts all three files
  2. Transforms in dependency order (customers first — orders need their IDs)
  3. Loads clean + quarantine rows in one atomic transaction
  4. Logs a final summary of rows processed
"""
import time

from src.config import Config
from src.etl.extract import extract_customers, extract_orders, extract_order_items
from src.etl.transform import transform_customers, transform_orders, transform_order_items
from src.etl.load import load_all, truncate_all
from src.logger import get_logger

log = get_logger(__name__)


def run(cfg: Config) -> None:
    t0 = time.perf_counter()
    log.info("=" * 60)
    log.info("PIPELINE START")
    log.info("=" * 60)

    # ── Extract ───────────────────────────────────────────────────────────────
    raw_customers   = extract_customers(cfg)
    raw_orders      = extract_orders(cfg)
    raw_items       = extract_order_items(cfg)

    # ── Transform (order matters: customers before orders before items) ───────
    clean_customers, q_customers = transform_customers(raw_customers, cfg)

    valid_customer_ids = set(clean_customers["customer_id"].dropna().astype(int).tolist())
    clean_orders, q_orders = transform_orders(raw_orders, valid_customer_ids, cfg)

    valid_order_ids = set(clean_orders["order_id"].dropna().astype(int).tolist())
    clean_items, q_items = transform_order_items(raw_items, valid_order_ids, cfg)

    # ── Load (one atomic transaction) ─────────────────────────────────────────
    results = load_all(
        cfg,
        customers=clean_customers,
        orders=clean_orders,
        order_items=clean_items,
        q_customers=q_customers,
        q_orders=q_orders,
        q_order_items=q_items,
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info(f"  customers loaded:      {results.get('customers', 0)}")
    log.info(f"  orders loaded:         {results.get('orders', 0)}")
    log.info(f"  order_items loaded:    {results.get('order_items', 0)}")
    log.info(f"  customers quarantined: {results.get('quarantine_customers', 0)}")
    log.info(f"  orders quarantined:    {results.get('quarantine_orders', 0)}")
    log.info(f"  items quarantined:     {results.get('quarantine_order_items', 0)}")
    log.info(f"  total duration_ms:     {elapsed}")
    log.info("=" * 60)


def truncate(cfg: Config) -> None:
    """Wipe all tables — used by `python main.py truncate --yes`."""
    truncate_all(cfg)
