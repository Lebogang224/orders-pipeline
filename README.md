# Orders Pipeline

End-to-end ETL pipeline for e-commerce order data.
Reads messy CSV/JSONL files, validates and cleans them with pandas,
loads clean rows into PostgreSQL via `COPY`, and quarantines bad rows with explicit reasons.

Built as a LexisNexis / OfferZen Data Engineer assessment.

---

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 17 (local or Docker)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Edit `config.yaml` or set environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=orders
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres123
```

The defaults in `config.yaml` work out of the box for a local PostgreSQL install.

### 3. Create the database

```sql
-- In psql:
CREATE DATABASE orders TEMPLATE template0;
```

### 4. Initialise schema

```bash
python main.py init
```

This applies `src/db/schema.sql` (tables + constraints) and `src/db/views.sql` (analytics views).

### 5. Run the pipeline

```bash
python main.py run
```

For a safe re-run (truncates all tables first, then loads):

```bash
python main.py run --fresh
```

### 6. Generate a report

```bash
python main.py report
```

Writes `REPORT.md`. If `GROQ_API_KEY` is set, uses Groq (llama-3.3-70b-versatile) for an
AI-written narrative. Otherwise writes structured Markdown tables.

### 7. Reset (dev/test)

```bash
python main.py truncate --yes
```

---

## Project Structure

```
orders-pipeline/
├── config.yaml              # All config; supports ${VAR:-default} env interpolation
├── main.py                  # CLI entry point (init / run / report / truncate)
├── requirements.txt
│
├── data/                    # Sample source files (committed for reviewers)
│   ├── customers.csv        # 6 rows incl. invalid email + duplicate
│   ├── orders.jsonl         # 10 rows incl. unknown FK + invalid status
│   └── order_items.csv      # 12 rows incl. zero quantity + zero price
│
├── src/
│   ├── config.py            # Pydantic config models + load_config()
│   ├── logger.py            # Structured logging setup
│   │
│   ├── db/
│   │   ├── connection.py    # psycopg v3 context manager (commit/rollback)
│   │   ├── schema.sql       # Tables, constraints, indexes
│   │   └── views.sql        # Analytics + data-quality views
│   │
│   ├── etl/
│   │   ├── extract.py       # Read CSV / JSONL → raw DataFrames
│   │   ├── quarantine.py    # split() helper — routes bad rows to quarantine
│   │   ├── transform.py     # Clean + validate each entity
│   │   ├── load.py          # psycopg COPY bulk-load + _clean_row NaN handling
│   │   └── pipeline.py      # Orchestrator: extract → transform → load
│   │
│   └── agent/
│       └── report_agent.py  # LangChain + Groq report generator (graceful fallback)
│
├── tests/
│   ├── test_quarantine.py   # 9 unit tests — pure DataFrame, no DB
│   └── test_transform.py    # 25 unit tests — pure DataFrame, no DB
│
└── docs/
    ├── HIGH_LEVEL_DESIGN.md / .pdf
    ├── LOW_LEVEL_DESIGN.md  / .pdf
    └── SOLUTION.md          / .pdf  (trade-off decisions)
```

---

## Architecture Overview

```
customers.csv ──┐
orders.jsonl   ─┤─► Extract ──► Transform ──► Load (COPY) ──► PostgreSQL
order_items.csv ┘                   │
                                    └──► Quarantine tables
                                         (bad rows with reason + source_file)
```

**Key design decisions:**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Bulk load | `psycopg COPY` | 10-100x faster than `INSERT` |
| Status field | `TEXT + CHECK` | Easier to evolve than `ENUM` |
| Bad rows | Quarantine tables | Auditability — never silently drop data |
| Dedup strategy | Keep earliest `signup_date` | Deterministic, reproducible |
| Timestamps | Normalise to UTC | Eliminates timezone ambiguity downstream |
| Config | `${VAR:-default}` YAML | Works out of the box; no `.env` required |

---

## Database Schema

### Clean tables

| Table | Key columns |
|-------|-------------|
| `customers` | `customer_id` PK, `email` UNIQUE + CHECK lowercase, `signup_date`, `is_active` |
| `orders` | `order_id` PK, FK → customers, `status` CHECK in allowed values, `order_ts` TIMESTAMPTZ |
| `order_items` | PK (`order_id`, `line_no`), FK → orders, `quantity` > 0, `unit_price` > 0 |

### Quarantine tables

Same columns as clean tables but all `TEXT`, plus:
- `quarantine_reason` — why the row was rejected
- `source_file` — which input file it came from
- `ingested_at` — when it was quarantined

### Analytics views

| View | Description |
|------|-------------|
| `v_daily_metrics` | Daily order count, revenue, average order value |
| `v_top_customers` | Top 10 customers by lifetime spend (RANK) |
| `v_top_skus` | Top 10 SKUs by revenue and units sold (dual ranking) |
| `v_dq_duplicate_emails` | Emails appearing in customers more than once |
| `v_dq_orders_missing_customer` | Orders with no matching customer |
| `v_quarantine_summary` | Quarantine row counts grouped by table + reason |

---

## Running Tests

```bash
pytest tests/ -v
```

```
35 passed in ~19s
```

Tests cover all validation rules, edge cases, and the quarantine split logic.
No database connection required — pure DataFrame tests.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `orders` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres123` | Database password |
| `GROQ_API_KEY` | _(none)_ | Optional — enables AI report narrative |

---

## AI Usage

This project was built with AI assistance (Claude). All prompts are documented in `PROMPTS.md`
as required by the assessment brief.
