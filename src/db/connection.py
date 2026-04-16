"""
Database connection factory using psycopg v3.

Usage:
    from src.db.connection import get_connection
    with get_connection(cfg) as conn:
        conn.execute("SELECT 1")

Design notes:
- autocommit=False by default — callers manage transactions explicitly
- get_connection() is a context manager: commits on clean exit, rolls back on exception
- No connection pool at this scale — a single connection per pipeline run is sufficient
"""
from contextlib import contextmanager
from typing import Generator

import psycopg

from src.config import Config
from src.logger import get_logger

log = get_logger(__name__)


@contextmanager
def get_connection(cfg: Config) -> Generator[psycopg.Connection, None, None]:
    """
    Yield a psycopg v3 connection.
    Commits on clean exit, rolls back on exception, always closes.
    """
    conninfo = cfg.database.conninfo
    log.debug(f"Connecting to {cfg.database.host}:{cfg.database.port}/{cfg.database.database}")

    conn = psycopg.connect(conninfo)
    try:
        yield conn
        conn.commit()
        log.debug("Transaction committed")
    except Exception:
        conn.rollback()
        log.warning("Transaction rolled back")
        raise
    finally:
        conn.close()
        log.debug("Connection closed")
