"""
Agentic REPORT.md generator — stretch feature.

Uses LangChain + Groq (llama-3.3-70b-versatile) to query the database views
and write a plain-English executive summary as REPORT.md.

Degrades gracefully:
  - No GROQ_API_KEY        → skips LLM, writes a template report from raw SQL
  - No DB connection       → writes a placeholder with instructions
  - LLM call fails         → falls back to template report

Usage:
    python main.py report
"""
import os
import textwrap
from datetime import datetime
from pathlib import Path

from src.config import Config
from src.logger import get_logger

log = get_logger(__name__)

_REPORT_PATH = Path("REPORT.md")


# =============================================================================
# PUBLIC ENTRY POINT
# =============================================================================

def generate_report(cfg: Config) -> None:
    """Generate REPORT.md from analytics views, using LLM if available."""
    log.info("step=generate_report starting")

    metrics = _fetch_metrics(cfg)

    if cfg.agent.enabled and os.getenv("GROQ_API_KEY"):
        try:
            _write_llm_report(metrics, cfg)
            log.info("step=generate_report mode=llm status=ok")
            return
        except Exception as exc:
            log.warning(f"step=generate_report llm_failed={exc!r} falling_back_to_template")

    _write_template_report(metrics)
    log.info("step=generate_report mode=template status=ok")


# =============================================================================
# DATA FETCHING
# =============================================================================

def _fetch_metrics(cfg: Config) -> dict:
    """Query analytics views; return dict of results (empty on error)."""
    try:
        from src.db.connection import get_connection
        import psycopg
        from psycopg.rows import dict_row

        results = {}
        with get_connection(cfg) as conn:
            with conn.cursor(row_factory=dict_row) as cur:

                cur.execute("SELECT * FROM v_daily_metrics ORDER BY order_date DESC LIMIT 7")
                results["daily_metrics"] = cur.fetchall()

                cur.execute("SELECT * FROM v_top_customers LIMIT 5")
                results["top_customers"] = cur.fetchall()

                cur.execute("SELECT * FROM v_top_skus LIMIT 5")
                results["top_skus"] = cur.fetchall()

                cur.execute("SELECT * FROM v_quarantine_summary")
                results["quarantine_summary"] = cur.fetchall()

                # Totals
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM customers)    AS total_customers,
                        (SELECT COUNT(*) FROM orders)       AS total_orders,
                        (SELECT COUNT(*) FROM order_items)  AS total_items,
                        (SELECT COALESCE(SUM(total_amount),0) FROM orders) AS total_revenue
                """)
                results["totals"] = cur.fetchone()

        log.info(f"step=fetch_metrics ok tables={list(results.keys())}")
        return results

    except Exception as exc:
        log.warning(f"step=fetch_metrics failed={exc!r}")
        return {}


# =============================================================================
# LLM REPORT
# =============================================================================

def _write_llm_report(metrics: dict, cfg: Config) -> None:
    """Call Groq via LangChain and write the LLM-generated report."""
    from langchain_groq import ChatGroq
    from langchain_core.messages import SystemMessage, HumanMessage

    llm = ChatGroq(
        model=cfg.agent.model,
        temperature=0.3,
        api_key=os.environ["GROQ_API_KEY"],
    )

    prompt = _build_prompt(metrics)

    messages = [
        SystemMessage(content=(
            "You are a senior data analyst writing a concise executive summary "
            "of an e-commerce orders pipeline run. Write in clear, professional "
            "prose. Use Markdown. Do not fabricate numbers — only use the data provided."
        )),
        HumanMessage(content=prompt),
    ]

    response = llm.invoke(messages)
    content = response.content

    # Prepend metadata header
    header = _report_header(mode="AI-generated (Groq llama-3.3-70b-versatile)")
    _REPORT_PATH.write_text(header + "\n\n" + content, encoding="utf-8")


def _build_prompt(metrics: dict) -> str:
    totals = metrics.get("totals") or {}
    daily  = metrics.get("daily_metrics") or []
    top_c  = metrics.get("top_customers") or []
    top_s  = metrics.get("top_skus") or []
    q_sum  = metrics.get("quarantine_summary") or []

    lines = [
        "Write a REPORT.md executive summary for this ETL pipeline run.",
        "",
        "## Pipeline totals",
        f"- Customers loaded: {totals.get('total_customers', 'N/A')}",
        f"- Orders loaded:    {totals.get('total_orders', 'N/A')}",
        f"- Order items:      {totals.get('total_items', 'N/A')}",
        f"- Total revenue:    R{totals.get('total_revenue', 'N/A')}",
        "",
        "## Last 7 days of orders (date | orders | revenue | avg_order_value)",
    ]
    for row in daily:
        lines.append(
            f"  {row.get('order_date')} | {row.get('orders_count')} | "
            f"R{row.get('total_revenue')} | R{row.get('average_order_value')}"
        )

    lines += ["", "## Top 5 customers by lifetime spend (rank | email | spend | orders)"]
    for row in top_c:
        lines.append(
            f"  #{row.get('spend_rank')} {row.get('email')} — "
            f"R{row.get('lifetime_spend')} over {row.get('order_count')} orders"
        )

    lines += ["", "## Top 5 SKUs (sku | revenue | units_sold)"]
    for row in top_s:
        lines.append(
            f"  {row.get('sku')} — R{row.get('revenue')} / {row.get('units_sold')} units"
        )

    lines += ["", "## Data quality (quarantine summary)"]
    for row in q_sum:
        lines.append(
            f"  [{row.get('entity')}] {row.get('quarantine_reason')}: "
            f"{row.get('rejected_rows')} rows"
        )

    lines += [
        "",
        "Write 3-4 paragraphs: pipeline health, revenue highlights, "
        "top customers, data quality observations.",
    ]
    return "\n".join(lines)


# =============================================================================
# TEMPLATE REPORT (no LLM)
# =============================================================================

def _write_template_report(metrics: dict) -> None:
    """Write a structured Markdown report from raw metrics (no LLM)."""
    totals = metrics.get("totals") or {}
    daily  = metrics.get("daily_metrics") or []
    top_c  = metrics.get("top_customers") or []
    top_s  = metrics.get("top_skus") or []
    q_sum  = metrics.get("quarantine_summary") or []

    lines = [_report_header(mode="template (set GROQ_API_KEY to enable AI narrative)")]

    lines += [
        "",
        "## Pipeline Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Customers loaded | {totals.get('total_customers', '—')} |",
        f"| Orders loaded | {totals.get('total_orders', '—')} |",
        f"| Order items loaded | {totals.get('total_items', '—')} |",
        f"| Total revenue | R{totals.get('total_revenue', '—')} |",
        "",
        "## Daily Metrics (last 7 days)",
        "",
        "| Date | Orders | Revenue | Avg Order Value |",
        "|------|--------|---------|-----------------|",
    ]
    for row in daily:
        lines.append(
            f"| {row.get('order_date')} "
            f"| {row.get('orders_count')} "
            f"| R{row.get('total_revenue')} "
            f"| R{row.get('average_order_value')} |"
        )

    lines += [
        "",
        "## Top 5 Customers by Lifetime Spend",
        "",
        "| Rank | Email | Lifetime Spend | Orders |",
        "|------|-------|---------------|--------|",
    ]
    for row in top_c:
        lines.append(
            f"| #{row.get('spend_rank')} "
            f"| {row.get('email')} "
            f"| R{row.get('lifetime_spend')} "
            f"| {row.get('order_count')} |"
        )

    lines += [
        "",
        "## Top 5 SKUs by Revenue",
        "",
        "| SKU | Total Revenue | Units Sold | Revenue Rank | Units Rank |",
        "|-----|--------------|------------|--------------|------------|",
    ]
    for row in top_s:
        lines.append(
            f"| {row.get('sku')} "
            f"| R{row.get('revenue')} "
            f"| {row.get('units_sold')} "
            f"| #{row.get('revenue_rank')} "
            f"| #{row.get('units_rank')} |"
        )

    lines += [
        "",
        "## Data Quality — Quarantine Summary",
        "",
        "| Table | Reason | Rows Rejected |",
        "|-------|--------|---------------|",
    ]
    for row in q_sum:
        lines.append(
            f"| {row.get('entity')} "
            f"| `{row.get('quarantine_reason')}` "
            f"| {row.get('rejected_rows')} |"
        )

    if not q_sum:
        lines.append("| — | No quarantine rows | 0 |")

    lines += [
        "",
        "---",
        "_Report generated by orders-pipeline. "
        "To enable AI narrative, set `GROQ_API_KEY` and `agent.enabled: true` in config.yaml._",
    ]

    _REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


# =============================================================================
# HELPERS
# =============================================================================

def _report_header(mode: str) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    return textwrap.dedent(f"""\
        # Orders Pipeline — Run Report

        **Generated:** {now}
        **Mode:** {mode}
    """)
