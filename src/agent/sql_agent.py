"""
Conversational SQL Agent — stretch feature.

A REPL that lets any user ask plain-English questions about the pipeline data.
The agent inspects the schema, writes SQL, executes it, and answers in prose.

No SQL knowledge required from the user.

Usage:
    python main.py chat

Examples:
    > Who are my top 3 customers by lifetime spend?
    > How many orders were cancelled last month?
    > Are there any data quality issues I should know about?
    > What is the total revenue per day this week?
    > Which SKU sold the most units?

Requires:
    GROQ_API_KEY environment variable set.
    agent.enabled: true in config.yaml.
"""
import os
import sys

from src.config import Config
from src.logger import get_logger

log = get_logger(__name__)

# Tables + views the agent is allowed to see.
# Ordered so the agent's schema inspection reads cleanly.
_INCLUDE_TABLES = [
    # Clean tables
    "customers",
    "orders",
    "order_items",
    # Analytics views
    "v_daily_metrics",
    "v_top_customers",
    "v_top_skus",
    # Data quality views
    "v_dq_duplicate_emails",
    "v_dq_orders_missing_customer",
    "v_dq_invalid_order_items",
    "v_dq_invalid_order_status",
    "v_quarantine_summary",
    # Quarantine tables (useful for DQ questions)
    "quarantine_customers",
    "quarantine_orders",
    "quarantine_order_items",
]

_SYSTEM_PREFIX = """
You are a data analyst assistant for an e-commerce orders pipeline.
You have access to a PostgreSQL database with the following structure:

CLEAN TABLES:
- customers       : customer master data (email, signup_date, country_code, is_active)
- orders          : order headers (status, order_ts in UTC, total_amount, currency)
- order_items     : order lines (sku, quantity, unit_price, category)

ANALYTICS VIEWS (pre-built, prefer these for speed):
- v_daily_metrics          : date, orders_count, total_revenue, average_order_value
- v_top_customers          : top 10 customers by lifetime spend (RANK)
- v_top_skus               : top 10 SKUs by revenue and units sold (dual RANK)

DATA QUALITY VIEWS:
- v_dq_duplicate_emails        : customers sharing the same email
- v_dq_orders_missing_customer : orders with no matching customer
- v_dq_invalid_order_items     : items with non-positive quantity or price
- v_dq_invalid_order_status    : orders with a status outside the allowed set
- v_quarantine_summary         : all rejected rows grouped by table and reason

QUARANTINE TABLES:
- quarantine_customers / quarantine_orders / quarantine_order_items
  (rows rejected during ETL, all TEXT columns + quarantine_reason + source_file)

Rules:
- Prefer the analytics views over raw tables when they answer the question.
- Always be concise. Lead with the answer, then supporting data.
- Format numbers with commas. Prefix currency values with R (ZAR).
- If a question cannot be answered from the data, say so clearly.
- Never modify data (no INSERT, UPDATE, DELETE, DROP).
"""


def run_chat(cfg: Config) -> None:
    """Start the interactive SQL agent REPL."""

    if not cfg.agent.enabled:
        print("Agent is disabled. Set agent.enabled: true in config.yaml.")
        sys.exit(1)

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("GROQ_API_KEY not set. Export it and try again:")
        print("  set GROQ_API_KEY=your_key_here   (Windows)")
        print("  export GROQ_API_KEY=your_key_here (Linux/Mac)")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Orders Pipeline — Data Agent")
    print("  Powered by Groq / LLaMA 3.3 70B")
    print("=" * 60)
    print("  Ask anything about your orders data.")
    print("  Type 'exit' or 'quit' to leave.\n")

    agent = _build_agent(cfg, api_key)

    while True:
        try:
            question = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not question:
            continue

        if question.lower() in ("exit", "quit", "bye", "q"):
            print("Goodbye.")
            break

        print()
        try:
            result = agent.invoke({"input": question})
            answer = result.get("output", str(result))
            print(answer)
        except Exception as exc:
            log.warning(f"agent_error={exc!r}")
            print(f"Sorry, I ran into an error: {exc}")
        print()


def _build_agent(cfg: Config, api_key: str):
    """Construct the LangChain SQL agent."""
    from langchain_groq import ChatGroq
    from langchain_community.utilities import SQLDatabase
    from langchain_community.agent_toolkits import create_sql_agent

    # SQLAlchemy DSN — psycopg3 dialect
    db_cfg = cfg.database
    dsn = (
        f"postgresql+psycopg://{db_cfg.user}:{db_cfg.password}"
        f"@{db_cfg.host}:{db_cfg.port}/{db_cfg.database}"
    )

    db = SQLDatabase.from_uri(
        dsn,
        include_tables=_INCLUDE_TABLES,
        sample_rows_in_table_info=2,   # show 2 example rows so the LLM understands the data
        view_support=True,             # include views, not just base tables
    )

    llm = ChatGroq(
        model=cfg.agent.model,
        temperature=0,                 # deterministic — we want accurate SQL, not creative
        api_key=api_key,
    )

    agent = create_sql_agent(
        llm=llm,
        db=db,
        agent_type="tool-calling",
        prefix=_SYSTEM_PREFIX,
        verbose=False,                 # set True to see the agent's chain-of-thought
        max_iterations=10,
        handle_parsing_errors=True,
    )

    log.info(f"sql_agent=ready model={cfg.agent.model} tables={len(_INCLUDE_TABLES)}")
    return agent
