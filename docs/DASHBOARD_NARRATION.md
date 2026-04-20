# Dashboard Narration — Cue Cards

> Read these while the dashboard is on screen. Each section matches a dashboard header.
> Keep this file open on a second monitor / phone while recording.

---

## INTRO — when the page first loads

This is an end-to-end view of one pipeline run — schema design, data cleaning,
bulk load, and analytics. Built for the LexisNexis / OfferZen Data Engineer Assessment.

---

## THE SOURCE DATA — before the pipeline runs

Three raw files arrive from upstream systems every day.
They're intentionally messy — mixed-case emails, duplicate records,
invalid status values, orphaned foreign keys, and zero-value line items.

A naive pipeline would crash on this data, or worse, silently load it corrupted.

This pipeline validates every row, quarantines the bad ones with an explicit reason,
and loads only the clean subset into PostgreSQL.

The tabs below show the data exactly as it arrives — problems and all.

---

### TAB 1 — customers.csv

Problems in this file:

- **Row 2** — email is mixed-case (`JOHN.Smith@Example.com`).
  Not invalid, but inconsistent.
  The pipeline lowercases and strips all emails before any check.

- **Rows 4 & 5** — two different rows share the same email address
  (`dup.email@example.com` / `dup.email@EXAMPLE.com`).
  After normalisation they are identical.
  The pipeline keeps the one with the earliest `signup_date`
  and quarantines the other with reason `duplicate_email`.

- **Row 6** — email is `bademail` with no `@` symbol.
  Fails the format check. Quarantined with reason `invalid_email_format`.

- **Row 3** — `country_code` is blank.
  Allowed — the pipeline treats this as NULL, not an error.

---

### TAB 2 — orders.jsonl

Problems in this file:

- **Row 3** — `customer_id` is `999`, which does not exist in the customers file.
  Quarantined with reason `missing_customer_fk`.

- **Row 4** — `status` is `processing`.
  The allowed set is `placed`, `shipped`, `cancelled`, `refunded`.
  Quarantined with reason `invalid_status`.

- **Rows 7 & 8** — `customer_id` values `6` and `5` map to customers
  that were quarantined upstream (bad email and duplicate).
  Because those customers never made it to the clean table,
  these orders have no valid parent and cascade into quarantine
  with reason `missing_customer_fk`.

- **Mixed timestamp formats across all rows** — some use `+02:00` offset,
  some use `Z`, one uses a space separator (`2024-03-03 11:30:00`),
  one uses slashes (`2024/03/04 12:00:00`).
  The pipeline normalises all of them to UTC in a single `pd.to_datetime` call.

---

### TAB 3 — order_items.csv

Problems in this file:

- **Row 5 (SKU D-333)** — `quantity` is `0`.
  A line item with zero quantity has no business meaning.
  Quarantined with reason `non_positive_quantity`.

- **Row 6 (SKU E-777)** — `unit_price` is `0.00`.
  Quarantined with reason `non_positive_unit_price`.

- **Row 10 (SKU H-655)** — `unit_price` is `0.00`.
  Same rule, same reason.

- **Rows 3, 7, 8 (SKUs C-100, G-321, part of order 1005)** —
  these line items belong to orders that were quarantined upstream.
  Because those orders never reached the clean table,
  these items have no valid parent and cascade into quarantine
  with reason `missing_order_fk`.

---

## STEP 1 — DATABASE SCHEMA

Before any data moves, the pipeline creates the full database with a single command:
`python main.py init`.

**Three clean tables** enforce constraints at rest — email format,
allowed status values, positive quantities, and foreign-key integrity.

**Three quarantine tables** mirror those tables with all columns as `TEXT`,
so a bad row is never rejected at the schema level.
Each quarantine table adds three columns: `quarantine_reason`, `source_file`, and `ingested_at`.

This command is idempotent — safe to run any number of times.

---

## STEP 2 — DATA CLEANING (Transform + Quarantine)

Every row is validated against business rules in the transform layer.
Bad rows are never deleted — they're quarantined with an explicit reason stamped on them.

Rejections **cascade**: a quarantined customer causes their orders
to be quarantined too (`missing_customer_fk`),
and those orders' line items follow (`missing_order_fk`).

The quarantine tables become an audit log the data team can investigate and re-ingest from.

### Row Funnel — where did every row go?

*(reading the table left-to-right: rows in, loaded clean, quarantined)*

### Quarantine Breakdown — why each row was rejected

Every rejected row has exactly one reason.
Rows rejected upstream cascade downward —
a bad customer causes `missing_customer_fk` on their orders,
which causes `missing_order_fk` on their line items.

---

## STEP 3 — BULK LOAD

Clean rows land in PostgreSQL using psycopg v3's `COPY` protocol —
10 to 100 times faster than batched `INSERT` statements,
because it streams rows directly into the table without parsing individual SQL statements.

All six tables (3 clean + 3 quarantine) load inside a **single transaction**:
everything commits or nothing does.

If anything fails mid-run, the database rolls back completely —
the next run always starts from a known-clean state.

---

## STEP 4 — SQL ANALYTICS VIEWS

Eight views surface operational insights directly from the clean tables —
no application logic required.

`v_top_customers` and `v_top_skus` use the `RANK()` window function
to rank customers by lifetime spend and SKUs by revenue and units sold.

`v_daily_metrics` groups orders by day with a `WITH` CTE for clarity.

These views are the downstream interface: any BI tool, the report generator,
or this dashboard queries the views — not the raw tables.

### Daily Revenue

*(point at the bar chart — "this is daily revenue straight from v_daily_metrics")*

### Top 5 Customers and Top 5 SKUs

*("these two tables come from v_top_customers and v_top_skus — both use RANK() window functions")*

---

## CLOSING

To recap the senior-engineer signals:

One — **quarantine, not drop.** Bad data is visible and auditable.

Two — **COPY over INSERT.** The brief said performance matters. COPY is the right tool.

Three — **validation order is intentional.** Format checks first, business rules second,
FK checks last. That way you know exactly *why* a row was rejected, not just that it was.

Four — **everything is testable without a database.** The transform layer is pure functions —
DataFrames in, DataFrames out.

The repo is at `github.com/Lebogang224/orders-pipeline`. Thanks for watching.
