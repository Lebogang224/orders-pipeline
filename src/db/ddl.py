"""
DDL runner — applies schema.sql and views.sql to the database.

Called by: python main.py init

Design:
- Reads SQL files and executes them as single statements batches
- Both files are idempotent (IF NOT EXISTS / CREATE OR REPLACE)
- Safe to run multiple times without side effects
"""
import time
from pathlib import Path

from src.config import Config
from src.db.connection import get_connection
from src.logger import get_logger

log = get_logger(__name__)

_SQL_DIR = Path(__file__).parent


def apply_schema(cfg: Config) -> None:
    """Create all primary + quarantine tables and indexes."""
    _run_sql_file(cfg, _SQL_DIR / "schema.sql", label="schema")


def apply_views(cfg: Config) -> None:
    """Create or replace all analytics and data quality views."""
    _run_sql_file(cfg, _SQL_DIR / "views.sql", label="views")


def _run_sql_file(cfg: Config, path: Path, label: str) -> None:
    sql = path.read_text(encoding="utf-8")

    t0 = time.perf_counter()
    log.info(f"step=apply_{label} file={path.name}")

    with get_connection(cfg) as conn:
        # Execute the entire file as one batch — psycopg handles multi-statement
        conn.execute(sql)

    elapsed = int((time.perf_counter() - t0) * 1000)
    log.info(f"step=apply_{label} status=ok duration_ms={elapsed}")
