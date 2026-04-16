# AI Prompt Log

> OfferZen assessment requirement: *"Document any AI tools and prompts used."*
>
> All prompts below were issued to **Claude (Anthropic)** via Claude Code (VS Code extension).
> The AI acted as a pair programmer — suggesting code structure, writing boilerplate, and
> catching bugs — while design decisions, validation, and debugging were done collaboratively.

---

## 1. Initial Architecture Design

**Prompt:**
```
I have a LexisNexis / OfferZen Data Engineer assessment. Requirements:
- PostgreSQL schema (customers, orders, order_items) with constraints
- Python ETL pipeline: pandas + psycopg v3, reading CSV and JSONL
- Bad rows must go to quarantine tables (not silently dropped)
- SQL analytics views
- Python 3.11, PostgreSQL 17

Before writing code: produce a HIGH_LEVEL_DESIGN.md and LOW_LEVEL_DESIGN.md.
Same discipline as a real project — document first.
```

**Output:** `docs/HIGH_LEVEL_DESIGN.md` and `docs/LOW_LEVEL_DESIGN.md`

---

## 2. PostgreSQL Schema

**Prompt:**
```
Write src/db/schema.sql.
- customers: customer_id SERIAL PK, email UNIQUE + CHECK(email = lower(email)),
  signup_date DATE NOT NULL, country_code CHAR(2) nullable, is_active BOOLEAN
- orders: order_id SERIAL PK, FK → customers, status TEXT CHECK IN allowed values,
  order_ts TIMESTAMPTZ, total_amount NUMERIC(12,2) CHECK >= 0
- order_items: composite PK (order_id, line_no), FK → orders,
  quantity INT CHECK > 0, unit_price NUMERIC(10,2) CHECK > 0
- quarantine_customers/orders/order_items: same columns as clean tables but all TEXT,
  plus quarantine_reason TEXT, source_file TEXT, ingested_at TIMESTAMPTZ DEFAULT NOW()
- Indexes on orders.customer_id, orders.order_ts, order_items.sku
Use TEXT + CHECK for status instead of ENUM — easier schema evolution.
```

**Output:** `src/db/schema.sql`

---

## 3. Analytics Views

**Prompt:**
```
Write src/db/views.sql with six views:
1. v_daily_metrics — date, orders_count, total_revenue, average_order_value
2. v_top_customers — RANK() by lifetime spend, top 10 with order_count
3. v_top_skus — revenue_rank AND units_rank (two rankings), LIMIT 10
4. v_dq_duplicate_emails — emails that appear more than once
5. v_dq_orders_missing_customer — orders where customer FK is missing
6. v_quarantine_summary — UNION ALL across all three quarantine tables,
   grouped by source_table + quarantine_reason
```

**Output:** `src/db/views.sql`

---

## 4. Config System

**Prompt:**
```
Write src/config.py using Pydantic BaseModel (not BaseSettings).
- Read config.yaml with PyYAML
- Interpolate ${VAR:-default} placeholders from environment variables
- Nested models: DBConfig (with .dsn and .conninfo properties),
  FilesConfig, ETLConfig, LoggingConfig, AgentConfig
- load_config(path) factory function
No .env file needed — config.yaml defaults work out of the box.
```

**Output:** `src/config.py`

---

## 5. Database Connection Manager

**Prompt:**
```
Write src/db/connection.py — a context manager using psycopg v3.
- get_connection(cfg) yields an open psycopg.Connection
- On success: conn.commit()
- On exception: conn.rollback() then re-raise
- Always: conn.close()
Single transaction per context block.
```

**Output:** `src/db/connection.py`

---

## 6. Quarantine Helper

**Prompt:**
```
Write src/etl/quarantine.py with two functions:
1. split(df, mask, reason, source_file) → (clean_df, quarantine_df)
   - clean = df[mask]
   - bad = df[~mask], coerce all columns to str, add quarantine_reason + source_file
   - must NOT mutate the input df
2. add_reason(df, reason, source_file) → quarantine_df
   - marks the entire df as quarantined (used for bulk rejects)
```

**Output:** `src/etl/quarantine.py`

---

## 7. Transform Layer

**Prompt:**
```
Write src/etl/transform.py with three functions returning (clean, quarantine):

transform_customers(df, cfg):
  1. Strip + lowercase email
  2. Reject invalid email format (regex: must have @, non-empty parts, dot in domain)
  3. Parse signup_date → date, reject unparseable
  4. Cast is_active → bool (handle true/false/1/0 strings)
  5. Null country_code: replace "" with None; quarantine if allow_null_country_code=false
  6. Dedup emails: keep row with earliest signup_date
  7. Cast customer_id → Int64

transform_orders(df, valid_customer_ids, cfg):
  1. pd.to_datetime(errors='coerce', utc=True, format='mixed') — handles ISO-8601,
     space-separated, slash-delimited formats
  2. Reject unparseable timestamps
  3. Validate status ∈ {placed, shipped, cancelled, refunded}
  4. Validate customer_id FK against valid_customer_ids set
  5. Cast total_amount, reject negative

transform_order_items(df, valid_order_ids, cfg):
  1. Cast quantity → numeric, reject <= 0
  2. Cast unit_price → numeric, reject <= 0
  3. Validate order_id FK against valid_order_ids set

All quarantine decisions use quarantine.split(). Validation order: format → business rules → FK.
```

**Output:** `src/etl/transform.py`

---

## 8. Load Layer

**Prompt:**
```
Write src/etl/load.py using psycopg v3 cursor.copy() (not batched INSERT).
- load_all(cfg, customers, orders, order_items, q_customers, q_orders, q_order_items)
  → loads all six DataFrames in one transaction, returns {table: rows_loaded}
- _copy_df(conn, table, df, columns) — COPY df → table using FORMAT TEXT, NULL ''
- _clean_row(row) — converts float NaN and string "nan"/"NaT"/"None"/"<NA>" to None
  (psycopg COPY sends None as SQL NULL via the NULL '' directive)
- truncate_all(cfg) — utility for dev/test

COPY is 10-100x faster than INSERT. Pre-validate in pandas so the COPY batch is clean.
```

**Output:** `src/etl/load.py`

**Bug caught during testing:** `_clean_row` was needed because `out.where(pd.notnull(out), other=None)` does not fully prevent NaN from appearing during `itertuples()`. The explicit helper catches `isinstance(v, float) and math.isnan(v)`.

---

## 9. Pipeline Orchestrator

**Prompt:**
```
Write src/etl/pipeline.py — orchestrates extract → transform → load.
- Passes valid_customer_ids set from clean customers into transform_orders
- Passes valid_order_ids set from clean orders into transform_order_items
- Calls load_all in one atomic transaction
- Logs a PIPELINE COMPLETE summary with all row counts and total duration_ms
```

**Output:** `src/etl/pipeline.py`

---

## 10. CLI Entry Point

**Prompt:**
```
Write main.py using argparse with four subcommands:
- init   → apply schema.sql + views.sql
- run    → run the full pipeline
- report → generate REPORT.md
- truncate --yes → truncate all tables (requires explicit --yes flag)
Load config from config.yaml. Set up logging from cfg.logging settings.
```

**Output:** `main.py`

---

## 11. Unit Tests

**Prompt:**
```
Write tests/test_quarantine.py and tests/test_transform.py.
No database needed — pure DataFrame tests.

test_quarantine.py: TestSplit (7 tests), TestAddReason (2 tests)
  - clean/quarantine split, reason attachment, source_file, all-valid, all-invalid,
    str coercion, no mutation of input df

test_transform.py: TestTransformCustomers (10), TestTransformOrders (9), TestTransformOrderItems (6)
  - email normalisation (lowercase, whitespace)
  - invalid email quarantined
  - invalid date quarantined
  - duplicate email: keep earliest, case-insensitive
  - null country_code allowed by config
  - is_active cast to bool
  - status validation (all 4 valid values)
  - FK validation (unknown customer / unknown order)
  - datetime format variations: ISO-8601 with TZ, space-separated, slash-delimited
  - UTC normalisation: +02:00 and Z timestamps should resolve to same UTC moment
  - zero/negative quantity quarantined
  - zero unit_price quarantined
```

**Output:** `tests/test_quarantine.py`, `tests/test_transform.py`

**Fixes applied after first run (35 tests, 2 initially failed):**
- `assert clean.iloc[0]["country_code"] is None` → `assert pd.isna(...)` because pandas stores `None` as `NaN` in object columns
- `assert clean.iloc[0]["is_active"] is False` → `assert clean.iloc[0]["is_active"] == False` because `.astype(bool)` returns `numpy.bool_`, not Python's `False` singleton

---

## 12. Agentic Report Generator

**Prompt:**
```
Write src/agent/report_agent.py.
- generate_report(cfg) is the entry point
- Query v_daily_metrics, v_top_customers, v_top_skus, v_quarantine_summary
- If GROQ_API_KEY is set and agent.enabled=true: call Groq via LangChain
  (ChatGroq, llama-3.3-70b-versatile) to write a narrative executive summary
- If no API key or LLM call fails: write a structured Markdown table report
- Always write to REPORT.md
Graceful degradation is required — the pipeline must run without a Groq key.
```

**Output:** `src/agent/report_agent.py`

---

## 13. Documentation PDFs

**Prompt:**
```
Convert these Markdown files to PDFs using reportlab:
1. docs/HIGH_LEVEL_DESIGN.md → docs/HIGH_LEVEL_DESIGN.pdf
2. docs/LOW_LEVEL_DESIGN.md  → docs/LOW_LEVEL_DESIGN.pdf
3. SOLUTION.md               → docs/SOLUTION.pdf

Professional styling: dark navy header, clean body font, monospace code blocks.
Include title and author from each document's header.
```

**Output:** `docs/generate_pdfs.py`, three PDF files

**Bug caught:** `LayoutError` — single-cell Table for code blocks was too tall for one page.
Fixed by splitting code into a one-row-per-line Table with `splitByRow=True`.

---

## Summary

| Area | Files AI-assisted | Human decisions |
|------|-------------------|-----------------|
| Schema design | `schema.sql`, `views.sql` | Chose TEXT+CHECK over ENUM; quarantine pattern |
| Config | `config.py` | `${VAR:-default}` YAML interpolation approach |
| ETL core | `extract.py`, `transform.py`, `quarantine.py` | Validation order; dedup strategy |
| Load | `load.py` | COPY over INSERT; _clean_row NaN handling |
| Tests | `test_transform.py`, `test_quarantine.py` | Test coverage strategy |
| Docs | `HLD.md`, `LLD.md`, `SOLUTION.md` | Trade-off rationale in SOLUTION.md |

All generated code was reviewed, tested, and debugged interactively.
