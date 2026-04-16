# Orders Data Pipeline — High-Level Design

**Project:** Orders ETL Pipeline (OfferZen / LexisNexis Technical Vetting)
**Author:** Lebogang Mphaga
**Date:** 2026-04-14
**Version:** 1.0

---

## 1. Executive Summary

A maintainable, production-style data pipeline that ingests messy raw order files (CSV + JSONL), validates and cleans them, loads them into PostgreSQL, and surfaces operational analytics as SQL views.

The design priorities — in order — are:

1. **Reliability** — bad data is quarantined, not silently dropped
2. **Clarity** — every transformation step is logged and reproducible
3. **Operability** — one command brings up the full stack; one command runs the pipeline
4. **Correctness** — schema constraints + SQL checks enforce data standards at rest

---

## 2. Problem Statement

A data team receives three daily files — `customers.csv`, `orders.jsonl`, `order_items.csv` — from upstream systems. The files are known to contain:

- Mixed-case and duplicate email addresses
- Invalid email formats
- Non-standard datetime formats (mixed ISO-8601, space-separated, slash-delimited)
- Invalid `status` values (outside the allowed enum)
- Orders referencing non-existent `customer_id`
- Non-positive `quantity` and `unit_price` on line items

A brittle pipeline would crash or silently corrupt the warehouse. The expected solution **validates, quarantines bad rows, and proceeds with the clean subset** — producing a reliable dataset for downstream analytics.

---

## 3. System Architecture

```
+----------------------+           +----------------------+
|   Raw Files (data/)  |           |   Config (YAML)      |
|   - customers.csv    |           |   - DSN              |
|   - orders.jsonl     |           |   - file paths       |
|   - order_items.csv  |           |   - thresholds       |
+----------+-----------+           +----------+-----------+
           |                                  |
           v                                  v
+-----------------------------------------------------------+
|                     Python ETL (main.py)                  |
|                                                           |
|   EXTRACT  -->  TRANSFORM  -->  QUALITY  -->  LOAD        |
|   (pandas)      (pandas)        (checks)      (psycopg v3 |
|                                                COPY)      |
|                                                           |
+-----+-----------------------------+-----------------------+
      |                             |
      | valid rows                  | invalid rows
      v                             v
+-------------------+        +--------------------------+
|                   |        |                          |
|   PostgreSQL      |        |  quarantine_* tables     |
|   - customers     |        |  (same shape + reason    |
|   - orders        |        |   + ingested_at)         |
|   - order_items   |        |                          |
|                   |        +--------------------------+
+---------+---------+
          |
          v
+-------------------------+
|   SQL Views             |
|   - daily_metrics       |
|   - top_customers       |
|   - top_skus            |
|   - dq_duplicate_emails |
|   - dq_missing_customer |
+-----------+-------------+
            |
            v
+-------------------------+
|   REPORT.md (stretch)   |
|   Agentic summary via   |
|   Groq + LangChain      |
+-------------------------+
```

---

## 4. Technology Choices

| Layer | Tech | Why |
|---|---|---|
| **Language** | Python 3.11 | Within "3.10+" required; stable, broad library support |
| **Ingestion** | pandas | Prescribed; handles CSV + JSONL + dtype coercion natively |
| **Database driver** | psycopg v3 | Prescribed; modern async-capable, clean API, `COPY` support |
| **Database** | PostgreSQL 16 | Within "14+"; latest stable LTS |
| **DDL** | SQLAlchemy 2 Core | Optional in brief; cleaner than raw SQL for DDL, but we emit plain SQL via the Core dialect so the output is driver-agnostic |
| **Config** | Pydantic Settings + YAML | Type-safe config with env-var overrides; YAML for human editing |
| **Logging** | Python `logging` (stdlib) | Prescribed "equivalent" accepted; stdlib keeps dependency surface minimal |
| **Orchestration** | Plain Python CLI (`main.py`) | Brief asks for "single command"; no need for Airflow/Prefect at this scale |
| **Infra** | Docker Compose | `docker compose up` → Postgres ready. Makes the reviewer's setup 30 seconds |
| **Testing** | pytest | Unit tests on pure transforms; no DB needed for most |
| **Stretch (agent)** | LangChain + Groq (Llama 3.3 70B, free) | Used in my Legal AI project; reusable pattern |

### Why not alternatives

- **Why not Polars?** Brief explicitly says pandas.
- **Why not Alembic migrations?** Overkill for three tables with no schema evolution.
- **Why not Airflow?** This is a single-shot pipeline run, not a scheduled DAG. Cron + this CLI would suffice in production.
- **Why not async psycopg?** No I/O concurrency needed — bottleneck is COPY throughput, not connection juggling.

---

## 5. Data Flow

### 5.1 Extract
- `customers.csv` → `pd.read_csv` with `dtype=str` (defer typing to transform step for better error visibility)
- `orders.jsonl` → `pd.read_json(lines=True)`
- `order_items.csv` → `pd.read_csv`

### 5.2 Transform (per entity)

**Customers**
1. Lowercase + strip `email`
2. Validate email shape (regex `^[^@\s]+@[^@\s]+\.[^@\s]+$`) — invalid → quarantine
3. Cast `signup_date` to date — coerce errors → quarantine
4. Null `country_code` allowed (flagged via `ALLOW_NULL_COUNTRY_CODE` config)
5. Resolve duplicate emails: **keep earliest `signup_date`** (first wins deterministically)

**Orders**
1. Parse `order_ts` with `pd.to_datetime(errors='coerce', utc=True)` — handles mixed formats
2. Convert all timestamps to UTC (standardizes the "mixed timezone" problem)
3. Validate `status` against allowed set `{placed, shipped, cancelled, refunded}` — invalid → quarantine
4. Cast `total_amount` to `Decimal(12,2)`
5. Validate `customer_id` exists in clean customer set — missing FK → quarantine

**Order Items**
1. Filter `quantity > 0 AND unit_price > 0` — violators → quarantine
2. Validate `order_id` exists in clean orders — missing FK → quarantine

### 5.3 Quality
Before load, the pipeline emits counts:
- rows read
- rows passed
- rows quarantined (by reason)
- rows loaded

### 5.4 Load
- `COPY FROM STDIN` via `psycopg.Cursor.copy()` — 10-100× faster than INSERT batches
- Transactional: all three tables load or none do
- Quarantine tables loaded in the same transaction

---

## 6. Data Quality Strategy: The Quarantine Pattern

**Decision:** Bad rows go to `quarantine_<entity>` tables, not `/dev/null`.

**Why:**
- Auditable — data team can investigate rejects
- Reversible — once a root cause is fixed upstream, we can re-ingest from quarantine
- Transparent — dashboards show data quality trends over time
- Senior-engineer pattern — junior engineers drop bad rows; senior engineers preserve them

**Shape:** Each quarantine table has the source columns (as `TEXT` to preserve the raw value) plus:
- `quarantine_reason` (TEXT)
- `ingested_at` (TIMESTAMPTZ DEFAULT NOW())
- `source_file` (TEXT)

---

## 7. Non-Functional Requirements

| NFR | Target | How we meet it |
|---|---|---|
| **Reproducibility** | Same input → same output | Deterministic dedup rules, fixed seed nowhere needed |
| **Idempotency** | Re-running won't double-load | `TRUNCATE ... RESTART IDENTITY` in `init`; per-run quarantine gets a `batch_id` |
| **Observability** | Know what happened | Structured logs: `step`, `rows_in`, `rows_out`, `duration_ms` |
| **Setup time** | < 2 minutes | `docker compose up` + `python main.py init && python main.py run` |
| **Grading clarity** | Reviewer gets it in 5 min | README quickstart + SOLUTION.md trade-offs |

---

## 8. Deployment / Execution Model

This is a batch pipeline, not a service. Execution flow:

```
docker compose up -d          # bring up Postgres
python main.py init           # create schema + views
python main.py run            # ETL + load
python main.py report         # (stretch) generate REPORT.md
```

In production this CLI would be wrapped in:
- A cron/Airflow schedule
- Secrets from Vault/SSM (not a local `.env`)
- Alerting on non-zero exit or high quarantine rate
- A metrics sink (Prometheus / Datadog)

These are noted as out-of-scope for the assessment but called out in SOLUTION.md.

---

## 9. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Reviewer can't run Postgres | Medium | High | Docker Compose ships Postgres bundled |
| Python version mismatch | Low | Medium | Pin `requires-python = ">=3.10"`; Docker image uses 3.11 |
| Datetime parser misses a format | Medium | Medium | `errors='coerce'` + quarantine catches unparseable rows |
| Reviewer wants raw SQL, not SQLAlchemy | Low | Low | DDL kept as hand-written SQL in `schema.sql` — SQLAlchemy is optional/internal only |
| Agentic stretch needs API key reviewer doesn't have | High | Low | Feature-flagged; stubs out if `GROQ_API_KEY` is absent, still writes REPORT.md from SQL |

---

## 10. Out of Scope (Deliberately)

- Streaming / CDC ingestion (batch is sufficient for the brief)
- Schema migrations (no schema evolution)
- Incremental loads (full refresh is correct at this volume)
- Multi-tenant partitioning
- Retry / dead-letter queues (cron + idempotency covers this)
- BI tool integration (views are the interface)

These limitations — and how to extend past them — are noted in SOLUTION.md and the Loom video.
