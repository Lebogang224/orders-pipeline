"""
Orders Pipeline — Streamlit Dashboard

Visual dashboard of one pipeline run: schema → cleaning → bulk load → analytics.

Run with:
    streamlit run app.py

Narration for each section lives in docs/DASHBOARD_NARRATION.md —
keep that open on a second screen while presenting.
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
    st.stop()

t        = data["totals"]
q_df     = data["q_df"]
daily_df = data["daily_df"]
cust_df  = data["cust_df"]
sku_df   = data["sku_df"]

cust_total = t["clean_customers"] + t["q_customers"]
ord_total  = t["clean_orders"]    + t["q_orders"]
item_total = t["clean_items"]     + t["q_items"]


# ══════════════════════════════════════════════════════════════════════════════
# TITLE
# ══════════════════════════════════════════════════════════════════════════════
st.title("Orders Pipeline Dashboard")
st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 0 — THE RAW SOURCE DATA
# ══════════════════════════════════════════════════════════════════════════════
st.header("The Source Data")

tab1, tab2, tab3 = st.tabs(["customers.csv", "orders.jsonl", "order_items.csv"])

with tab1:
    raw_customers = pd.read_csv("data/customers.csv", dtype=str)
    st.dataframe(raw_customers, use_container_width=True, hide_index=True)

with tab2:
    raw_orders = pd.read_json("data/orders.jsonl", lines=True, dtype=str)
    st.dataframe(raw_orders, use_container_width=True, hide_index=True)

with tab3:
    raw_items = pd.read_csv("data/order_items.csv", dtype=str)
    st.dataframe(raw_items, use_container_width=True, hide_index=True)

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — SCHEMA
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 1 — Database Schema")

col1, col2, col3 = st.columns(3)
col1.metric(label="Clean tables",      value=3)
col2.metric(label="Quarantine tables", value=3)
col3.metric(label="Analytics views",   value=8)

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — DATA CLEANING
# ══════════════════════════════════════════════════════════════════════════════
st.header("Step 2 — Data Cleaning")

st.subheader("Row Funnel")

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

st.subheader("Quarantine Breakdown")

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

st.subheader("Daily Revenue")
if not daily_df.empty:
    chart_df = (
        daily_df
        .set_index("date")[["total_revenue"]]
        .rename(columns={"total_revenue": "Revenue (R)"})
    )
    st.bar_chart(chart_df)

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
