"""
Centralised logging setup.

Usage:
    from src.logger import get_logger
    log = get_logger(__name__)
    log.info("step=extract_customers rows=6 duration_ms=12")
"""
import logging
import sys
from src.config import LoggingConfig


_configured = False


def setup_logging(cfg: LoggingConfig) -> None:
    """Call once at startup (main.py) to configure root logger."""
    global _configured
    if _configured:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(cfg.format))

    root = logging.getLogger()
    root.setLevel(cfg.level.upper())
    root.addHandler(handler)

    # Quieten noisy third-party loggers
    for noisy in ("httpx", "httpcore", "urllib3", "groq", "openai"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Get a named logger. setup_logging() must be called first."""
    return logging.getLogger(name)
