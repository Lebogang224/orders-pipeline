"""
Orders Pipeline — Streamlit Dashboard

Walks through every stage of the pipeline visually:
  Step 1  Schema        — tables and views created
  Step 2  Data Cleaning — rows in / loaded / quarantined per entity + cascade
  Step 3  Bulk Load     — live counts and revenue from the database
  Step 4  Analytics     — daily metrics, top customers, top SKUs

Run with:
    streamlit run app.py
"""
import streamlit as st
import pandas as pd

from src.config import load_config
from src.db.connection import get_connection
from psycopg.rows import dict_row


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Orders Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Config + data fetching ─────────────────────────────────────────────────────
@st.cache_resource
def _cfg():
    return load_config("config.yaml")


@st.cache_data(ttl=30)
def _fetch():
    cfg = _cfg()
    try:
        with get_connection(cfg) as conn:
            with conn.cursor(row_factory=dict_row) as cur:

                # All counts in one round-trip
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM customers)              AS clean_customers,
                        (SELECT COUNT(*) FROM orders)                 AS clean_orders,
                        (SELECT COUNT(*) FROM order_items)            AS clean_items,
                        (SELECT COUNT(*) FROM quarantine_customers)   AS q_customers,
                        (SELECT COUNT(*) FROM quarantine_orders)      AS q_orders,
                        (SELECT COUNT(*) FROM quarantine_order_items) AS q_items,
                        (SELECT COALESCE(SUM(total_amount), 0)
                           FROM orders)                               AS total_revenue
                """)
                totals = cur.fetchone()

                cur.execute(
                    "SELECT entity, quarantine_reason, rejected_rows "
                    "FROM v_quarantine_summary ORDER BY entity, rejected_rows DESC"
                )
                q_df = pd.DataFrame(cur.fetchall())

                cur.execute(
                    "SELECT date, orders_count, total_revenue, average_order_value "
                    "FROM v_daily_metrics ORDER BY date"
                )
                daily_df = pd.DataFrame(cur.fetchall())

                cur.execute(
                    "SELECT spend_rank, email, lifetime_spend, order_count "
                    "FROM v_top_customers LIMIT 5"
                )
                cust_df = pd.DataFrame(cur.fetchall())

                cur.execute(
                    "SELECT revenue_rank, sku, revenue, units_sold "
                    "FROM v_top_skus LIMIT 5"
                )
                sku_df = pd.DataFrame(cur.fetchall())

        return dict(totals=totals, q_df=q_df, daily_df=daily_df,
                    cust_df=cust_df, sku_df=sku_df, error=None)

    except Exception as exc:
        return dict(totals=None, q_df=None, daily_df=None,
                    cust_df=None, sku_df=None, error=str(exc))


# ── Load data ──────────────────────────────────────────────────────────────────
data = _fetch()

if data["error"]:
    st.error(f"Database unavailable: {data['error']}")
    st.info(
        "Run `python main.py init && python main.py run` to populate the database, "
        "then refresh this page."
    )
    st.stop()

t        = data["totals"]
q_df     = data["q_df"]
daily_df = data["daily_df"]
cust_df  = data["cust_df"]
sku_df   = data["sku_df"]

# Derived totals (clean + quarantined = rows that came in)
cust_total = t["clean_customers"] + t["q_customers"]
ord_total  = t["clean_orders"]    + t["q_orders"]
item_total = t["clean_items"]     + t["q_items"]


# ══════════════════════════════════════════════════════════════════════════════
# TITLE
# ══════════════════════════════════════════════════════════════════════════════
st.title("Orders Pipeline Dashboard")
st.caption(
    "End-to-end view of one pipeline run — schema design, data cleaning, "
    "bulk load, and analytics. "
    "Built for the LexisNexis / OfferZen Data Engineer Assessment."
)
st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 0 — THE RAW SOURCE DATA
# ══════════════════════════════════════════════════════════════════════════════
st.header("The Source Data — Before the Pipeline Runs")
st.info(
    "Three raw files arrive from upstream systems every day. "
    "They are intentionally messy — mixed-case emails, duplicate records, "
    "invalid status values, orphaned foreign keys, and zero-value line items. "
    "A naive pipeline would crash on this data, or worse, silently load it corrupted. "
    "This pipeline validates every row, quarantines the bad ones with an explicit reason, "
    "and loads only the clean subset into PostgreSQL. "
    "The tabs below show the data exactly as it arrives — problems and all."
)

tab1, tab2, tab3 = st.tabs(["customers.csv", "orders.jsonl", "order_items.csv"])

with tab1:
    raw_customers = pd.read_csv("data/customers.csv", dtype=str)
    st.dataframe(raw_customers, use_container_width=True, hide_index=True)
    st.warning(
        "**Problems in this file:**\n\n"
        "- **Row 2** — email is mixed-case (`JOHN.Smith@Example.com`). "
        "Not invalid, but inconsistent. The pipeline lowercases and strips all emails before any check.\n\n"
        "- **Rows 4 & 5** — two different rows share the same email address "
        "(`dup.email@example.com` / `dup.email@EXAMPLE.com`). "
        "After normalisation they are identical. The pipeline keeps the one with the earliest "
        "`signup_date` and quarantines the other with reason `duplicate_email`.\n\n"
        "- **Row 6** — email is `bademail` with no `@` symbol. "
        "Fails the format check. Quarantined with reason `invalid_email_format`.\n\n"
        "- **Row 3** — `country_code` is blank. Allowed — the pipeline treats this as NULL, "
        "not an error."
    )

with tab2:
    raw_orders = pd.read_json("data/orders.jsonl", lines=True, dtype=str)
    st.dataframe(raw_orders, use_container_width=True, hide_index=True)
    st.warning(
        "**Problems in this file:**\n\n"
        "- **Row 3** — `customer_id` is `999`, which does not exist in the customers file. "
        "Quarantined with reason `missing_customer_fk`.\n\n"
        "- **Row 4** — `status` is `processing`. "
        "The allowed set is `placed`, `shipped`, `cancelled`, `refunded`. "
        "Quarantined with reason `invalid_status`.\n\n"
        "- **Rows 7 & 8** — `customer_id` values `6` and `5` map to customers that were "
        "quarantined upstream (bad email and duplicate). "
        "Because those customers never made it to the clean table, these orders have no valid "
        "parent and cascade into quarantine with reason `missing_customer_fk`.\n\n"
        "- **Mixed timestamp formats across all rows** — some use `+02:00` offset, "
        "some use `Z`, one uses a space separator (`2024-03-03 11:30:00`), "
        "one uses slashes (`2024/03/04 12:00:00`). "
        "The pipeline normalises all of them to UTC in a single `pd.to_datetime` call."
    )

with tab3:
    raw_items = pd.read_csv("data/order_items.csv", dtype=str)
    st.dataframe(raw_items, use_container_width=True, hide_index=True)
    st.warning(
        "**Problems in this file:**\n\n"
        "- **Row 5 (SKU D-333)** — `quantity` is `0`. "
        "A line item with zero quantity has no business meaning. "
        "Quarantined with reason `non_positive_quantity`.\n\n"
        "- **Row 6 (SKU E-777)** — `unit_price` is `0.00`. "
        "Quarantined with reason `non_positive_unit_price`.\n\n"
        "- **Row 10 (SKU H-655)** — `unit_price` is `0.00`. "
        "Same rule, same reason.\n\n"
        "- **Rows 3, 7, 8 (SKUs C-100, G-321, part of order 1005)** — these line items belong "
        "to orders that were quarantined upstream. "
        "Because those orders never reached the clean table, these items have no valid parent "
        "and cascade into quarantine with reason `missing_order_fk`."
    )

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — SCHEMA
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 1 — Database Schema")
st.info(
    "Before any data moves, the pipeline creates the full database with a single command: "
    "`python main.py init`. "
    "**Three clean tables** enforce constraints at rest — email format, allowed status values, "
    "positive quantities, and foreign-key integrity. "
    "**Three quarantine tables** mirror those tables with all columns as `TEXT`, so a bad row "
    "is never rejected at the schema level. Each quarantine table adds three columns: "
    "`quarantine_reason`, `source_file`, and `ingested_at`. "
    "This command is idempotent — safe to run any number of times."
)

col1, col2, col3 = st.columns(3)
col1.metric(
    label="Clean tables",
    value=3,
    help="customers · orders · order_items — with PK, FK, and CHECK constraints",
)
col2.metric(
    label="Quarantine tables",
    value=3,
    help="quarantine_customers · quarantine_orders · quarantine_order_items",
)
col3.metric(
    label="Analytics views",
    value=8,
    help=(
        "v_daily_metrics · v_top_customers · v_top_skus · "
        "v_dq_duplicate_emails · v_dq_orders_missing_customer · "
        "v_dq_invalid_order_items · v_dq_invalid_order_status · v_quarantine_summary"
    ),
)
st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — DATA CLEANING
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 2 — Data Cleaning (Transform + Quarantine)")
st.info(
    "Every row is validated against business rules in the transform layer. "
    "Bad rows are never deleted — they're quarantined with an explicit reason stamped on them. "
    "Rejections **cascade**: a quarantined customer causes their orders to be quarantined too "
    "(`missing_customer_fk`), and those orders' line items follow (`missing_order_fk`). "
    "The quarantine tables become an audit log the data team can investigate and re-ingest from."
)

# ── Row funnel ─────────────────────────────────────────────────────────────────
st.subheader("Row Funnel — Where Did Every Row Go?")

h = st.columns([2, 1, 1, 1])
h[0].markdown("**Entity**")
h[1].markdown("**Rows in**")
h[2].markdown("**Loaded clean**")
h[3].markdown("**Quarantined**")

for label, total, clean, quarantined in [
    ("Customers",   cust_total, t["clean_customers"], t["q_customers"]),
    ("Orders",      ord_total,  t["clean_orders"],    t["q_orders"]),
    ("Order Items", item_total, t["clean_items"],     t["q_items"]),
]:
    row = st.columns([2, 1, 1, 1])
    row[0].markdown(f"**{label}**")
    row[1].markdown(str(total))
    row[2].success(f"{clean}")
    if quarantined:
        row[3].warning(f"{quarantined}")
    else:
        row[3].success("0")

# ── Quarantine breakdown ───────────────────────────────────────────────────────
st.subheader("Quarantine Breakdown — Why Each Row Was Rejected")
st.caption(
    "Every rejected row has exactly one reason. "
    "Rows rejected upstream cascade downward — "
    "a bad customer causes missing_customer_fk on their orders, "
    "which causes missing_order_fk on their line items."
)

if not q_df.empty:
    st.dataframe(
        q_df.rename(columns={
            "entity":            "Table",
            "quarantine_reason": "Reason",
            "rejected_rows":     "Rows Rejected",
        }),
        hide_index=True,
        use_container_width=True,
    )
else:
    st.success("No quarantined rows.")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — BULK LOAD
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 3 — Bulk Load")
st.info(
    "Clean rows land in PostgreSQL using psycopg v3's `COPY` protocol — "
    "10–100x faster than batched `INSERT` statements because it streams rows "
    "directly into the table without parsing individual SQL statements. "
    "All six tables (3 clean + 3 quarantine) load inside a **single transaction**: "
    "everything commits or nothing does. "
    "If anything fails mid-run, the database rolls back completely — "
    "the next run always starts from a known-clean state."
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Customers loaded",    t["clean_customers"])
col2.metric("Orders loaded",       t["clean_orders"])
col3.metric("Order items loaded",  t["clean_items"])
col4.metric("Total revenue",       f"R{t['total_revenue']}")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 4 — SQL Analytics Views")
st.info(
    "Eight views surface operational insights directly from the clean tables — "
    "no application logic required. "
    "`v_top_customers` and `v_top_skus` use the `RANK()` window function to rank "
    "customers by lifetime spend and SKUs by revenue and units sold. "
    "`v_daily_metrics` groups orders by day with a `WITH` CTE for clarity. "
    "These views are the downstream interface: any BI tool, the report generator, "
    "or this dashboard queries the views — not the raw tables."
)

# ── Daily revenue chart ────────────────────────────────────────────────────────
st.subheader("Daily Revenue")
if not daily_df.empty:
    chart_df = (
        daily_df
        .set_index("date")[["total_revenue"]]
        .rename(columns={"total_revenue": "Revenue (R)"})
    )
    st.bar_chart(chart_df)
else:
    st.caption("No daily data available.")

# ── Top customers + top SKUs ───────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("Top 5 Customers by Lifetime Spend")
    if not cust_df.empty:
        st.dataframe(
            cust_df.rename(columns={
                "spend_rank":     "Rank",
                "email":          "Email",
                "lifetime_spend": "Lifetime Spend (R)",
                "order_count":    "Orders",
            }),
            hide_index=True,
            use_container_width=True,
        )

with right:
    st.subheader("Top 5 SKUs by Revenue")
    if not sku_df.empty:
        st.dataframe(
            sku_df.rename(columns={
                "revenue_rank": "Rank",
                "sku":          "SKU",
                "revenue":      "Revenue (R)",
                "units_sold":   "Units Sold",
            }),
            hide_index=True,
            use_container_width=True,
        )

st.divider()
st.caption(
    "Lebogang Mphaga · Orders Pipeline · LexisNexis / OfferZen Data Engineer Assessment"
)
