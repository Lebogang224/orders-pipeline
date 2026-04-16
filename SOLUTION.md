# Solution Notes — Orders Data Pipeline

**Author:** Lebogang Mphaga
**Assessment:** LexisNexis / OfferZen — Data Engineer Technical Vetting
**Date:** 2026-04-14

---

## 1. What I Built

A small, maintainable ETL pipeline that:

1. Reads three raw files (`customers.csv`, `orders.jsonl`, `order_items.csv`)
2. Validates and cleans each dataset
3. Quarantines bad rows (rather than dropping them silently)
4. Loads clean data into PostgreSQL using `COPY`
5. Surfaces analytics and data quality checks as SQL views
6. (Stretch) Generates a `REPORT.md` summary via a lightweight LangChain + Groq agent

Single command to run end-to-end:
```bash
docker compose up -d
python main.py init
python main.py run
```

---

## 2. Architecture Decision: Quarantine vs Drop

**Decision:** Bad rows go to `quarantine_<entity>` tables — not `/dev/null`, not an exception.

**Why this matters:**
Silently dropping bad rows is the #1 data quality mistake in pipelines. If upstream sends 10% invalid emails and nobody notices, the warehouse is quietly wrong. Quarantine tables make this visible and auditable.

Every quarantine row has:
- The original source data (as raw TEXT — preserves exactly what came in)
- `quarantine_reason` — machine-readable string (e.g. `invalid_email`, `missing_customer_fk`)
- `source_file` + `ingested_at` — for lineage

In production I'd add a quarantine rate alert: if >5% of customers are rejected, page the on-call.

---

## 3. Trade-off: `COPY` vs Batched `INSERT`

**Decision:** Use psycopg v3's `cursor.copy()` (client-side COPY) for all three tables.

**Why:**
- COPY is 10–100× faster than row-by-row inserts — one network round-trip vs N
- The brief explicitly preferred it: *"Prefer client-side COPY"*
- At larger volumes (millions of rows) this becomes critical

**The tradeoff:**
COPY is less flexible mid-stream. If one row fails, the entire COPY aborts. My solution pre-validates in pandas before calling COPY, so the batch reaching the DB is already clean. This is the correct pattern — validate early, load clean.

**Batched INSERT fallback** is implemented and commented in `load.py`. I'd use it only if COPY caused compatibility issues with the psycopg build.

---

## 4. Trade-off: How I Handled Each Type of Bad Data

### 4.1 Duplicate emails (customers 4 & 5: `dup.email@example.com`)

**Decision:** Keep the record with the **earliest `signup_date`**. Quarantine the later duplicate(s).

**Why:** The brief suggested this. More importantly, earliest signup = the "real" account. The later one is likely a re-registration or data entry error. This is deterministic — same input always produces same output.

**What I rejected:** Last-write-wins (non-deterministic if timestamps are equal), or keeping both (violates the unique email constraint we're enforcing).

### 4.2 Invalid email format (customer 6: `bademail`)

**Decision:** Quarantine. Reason: `invalid_email_format`.

**Why:** We can't normalize an email that has no `@`. We can't contact this customer. Loading it would violate the spirit of the data standard even if we hacked the constraint.

### 4.3 Invalid `status` value (order 1004: `processing`)

**Decision:** Quarantine. Reason: `invalid_status`.

**Why:** I chose quarantine over mapping to a default because silently mapping `processing` → `placed` would hide an upstream bug. The correct fix is upstream — this pipeline should surface the error, not mask it.

**What I rejected:** Mapping to a default (hides the problem), dropping (loses the order silently).

### 4.4 Orders referencing unknown `customer_id` (order 1003: `customer_id=999`)

**Decision:** Quarantine. Reason: `missing_customer_fk`.

**Why:** We cannot infer which customer this belongs to. Loading it would fail the FK constraint anyway. The quarantine row + log gives the data team visibility to fix it upstream.

### 4.5 Non-positive quantities / prices (order_items: qty=0 rows)

**Decision:** Quarantine. Reason: `non_positive_quantity` or `non_positive_unit_price`.

**Why:** A `quantity=0` line item has no business meaning. A `unit_price=0.00` item may indicate a data error or a comp — either way it should be reviewed, not silently loaded.

### 4.6 Missing `country_code` (customer 3: empty)

**Decision:** Load as `NULL`. Log a warning but do not quarantine.

**Why:** The schema allows `country_code` to be nullable. Missing country is not a blocker for loading a customer. I made this configurable (`allow_null_country_code: true` in config).

### 4.7 Mixed datetime formats (orders: ISO-8601, space-separated, slash-delimited)

**Decision:** Use `pd.to_datetime(errors='coerce', utc=True)` — handles all formats, standardises to UTC, coerces unparseable to `NaT`, which then gets quarantined.

**Why:** pandas' parser handles all five datetime formats in the sample data. Coercing to UTC at load time means the warehouse always stores UTC — no timezone ambiguity downstream.

---

## 5. Schema Decisions

### Email uniqueness without extensions

The brief said *"avoid extensions"* — so no `citext` or `pg_trgm`. I enforced this with:
- `CHECK (email = lower(email))` — prevents non-lowercase from being stored
- `UNIQUE (email)` — prevents duplicates on the stored lowercase value

The pipeline lowercases emails before load, so the CHECK is a last-line defence.

### `status` as TEXT + CHECK vs ENUM

I used `TEXT` with a `CHECK` constraint rather than a PostgreSQL `ENUM` type. Reason: ENUMs require `ALTER TYPE` to add new values — a DDL change in production. A CHECK constraint can be dropped and re-added with a new set of values much more easily.

### NOT NULL choices

- `email`, `full_name`, `signup_date` on customers: NOT NULL — a customer without these is not a valid customer
- `country_code`: nullable — genuinely optional in many systems
- All order and order_item core fields: NOT NULL — no order should exist without an amount, timestamp, or status

---

## 6. What I Did NOT Do (and Why)

| Thing not done | Reason |
|---|---|
| Incremental / CDC loading | Full refresh is correct at this scale and for this assessment |
| Alembic migrations | No schema evolution required; single setup step is sufficient |
| Async psycopg | No concurrency benefit — bottleneck is I/O throughput, not connections |
| Materialized views | Brief explicitly said *"keep as views (not materialized)"* |
| Airflow / Prefect | Over-engineering for a single-command pipeline |
| Row-level retry | Pre-validation ensures the COPY batch is clean; no mid-stream retries needed |

---

## 7. If I Had More Time

1. **Incremental loads** — track a `last_loaded_at` watermark; only ingest new rows
2. **Schema migrations** — Alembic for schema evolution as the data model grows
3. **Great Expectations / Soda** — declarative DQ rules instead of hand-coded checks
4. **Prometheus metrics** — expose quarantine rates as gauges for alerting
5. **Full integration test** — spin up an in-memory Postgres (pytest-docker) and test the full pipeline round-trip

---

## 8. Stretch: Agentic REPORT.md

I implemented a lightweight agent using LangChain + Groq (Llama 3.3 70B — free tier) that:

1. Queries the five analytics views
2. Serialises results to a structured prompt
3. Asks the LLM to write a narrative markdown summary of key metrics and notable data quality findings

This directly fulfils the brief's stretch task: *"Generate a brief markdown summary using an agentic approach."*

**Graceful degradation:** If `GROQ_API_KEY` is absent, the report is generated from a deterministic template using the same SQL data — no LLM required. The reviewer doesn't need an API key to get a REPORT.md.

**In production**, I would:
- Store the API key in AWS SSM / Vault, not a `.env`
- Cache the LLM output to avoid redundant API calls on re-runs
- Log token usage for cost tracking

---

## 9. AI Assistance

I used Claude (via Claude Code) and Groq API during this assessment. Per OfferZen's instructions, see `PROMPTS.md` for the key prompts used and my reasoning for accepting or modifying each output.

All architectural decisions — quarantine pattern, COPY vs INSERT, dedup strategy, schema constraints — were made by me and are documented above. AI was used to accelerate implementation, not to substitute for judgment.
