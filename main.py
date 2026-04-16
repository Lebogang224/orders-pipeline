"""
Orders Pipeline CLI.

Commands:
  python main.py init      -- create schema + views (idempotent)
  python main.py run       -- extract -> transform (incl. DQ) -> load
  python main.py report    -- generate REPORT.md (+ optional LLM agent)
  python main.py truncate  -- wipe all tables (dev only; requires --yes)
"""
import argparse
import sys

from src.config import load_config
from src.logger import setup_logging, get_logger


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orders ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "command",
        choices=["init", "run", "report", "truncate"],
        help="Pipeline command to execute",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive operations (required for truncate)",
    )
    args = parser.parse_args()

    # Load config + setup logging before anything else
    cfg = load_config(args.config)
    setup_logging(cfg.logging)
    log = get_logger(__name__)

    log.info(f"command={args.command} config={args.config}")

    if args.command == "init":
        from src.db.ddl import apply_schema, apply_views
        apply_schema(cfg)
        apply_views(cfg)
        log.info("Schema and views applied successfully")

    elif args.command == "run":
        from src.etl.pipeline import run
        run(cfg)

    elif args.command == "report":
        from src.agent.report_agent import generate_report
        generate_report(cfg)

    elif args.command == "truncate":
        if not args.yes:
            print("ERROR: truncate is destructive. Pass --yes to confirm.")
            sys.exit(1)
        from src.etl.pipeline import truncate
        truncate(cfg)
        log.info("All tables truncated")


if __name__ == "__main__":
    main()
