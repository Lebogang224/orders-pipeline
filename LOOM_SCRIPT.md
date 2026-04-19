# Loom Walkthrough Script — Orders Pipeline

**Target length:** 6–8 minutes
**Recording mode:** Loom · screen + webcam bubble + mic
**Delivery:** Read silently → look at screen → speak. Don't read word-for-word — paraphrase 70%, verbatim 30%.

---

## SEGMENT 1 — Intro + What This Is (~60–90s)

[Open on README.md in VS Code. Smile. Speak slowly.]

> Hi Karin, I'm Lebogang Mphaga, and this is my submission for the Data Engineer technical vetting.
>
> The brief was to build a small but production-style ETL pipeline that ingests three messy source files — customers, orders, and order items — validates them, loads the clean rows into PostgreSQL, and quarantines the bad ones with explicit reasons. I also wired up the stretch agentic feature using LangChain and Groq.
>
> I'll spend the next few minutes walking you through the architecture, then show the code, then run the whole pipeline live so you can see it work end to end.

[Switch to docs/HIGH_LEVEL_DESIGN.md, scroll to the architecture diagram in section 3.]

> The shape of the system is simple. Three raw files come in. They go through extract, transform with data quality checks built in, then load via Postgres COPY. Clean rows go to the main tables. Bad rows go to parallel quarantine tables that mirror the shape of the source — same columns plus a `quarantine_reason`, a `source_file`, and an `ingested_at` timestamp.
>
> The whole design priority was: never silently drop data. A junior engineer's pipeline drops bad rows. A senior engineer's pipeline preserves them so the data team can investigate, fix the upstream issue, and re-ingest.

---

## SEGMENT 2 — Code Walkthrough (~2–3 min)

[Open `main.py`.]

> Single CLI entry point. Four commands: `init` to create the schema and views, `run` for the full ETL, `report` for the executive summary, and `truncate` for dev resets — which requires `--yes` because it's destructive.

[Open `src/etl/transform.py`.]

> All validation lives in transform. For customers, I lowercase and strip emails, validate the format with a regex, cast `signup_date`, and resolve duplicate emails by keeping the earliest signup — that's deterministic, so re-runs always produce the same output.

[Scroll to `transform_orders`.]

> For orders, I parse `order_ts` with `pd.to_datetime` using `errors='coerce'` and force everything to UTC — that handles the mixed timezone problem from the brief in one line. I validate `status` against the allowed set, and I check that `customer_id` exists in the clean customer set. If a customer was quarantined, their orders cascade into quarantine too with reason `missing_customer_fk`.

[Open `src/etl/quarantine.py`.]

> This is the helper that splits a DataFrame on a boolean mask. Returns a `(clean, quarantine)` tuple with the reason and source file stamped on the rejected rows. It's used everywhere in transform, which is why every rejection has an explicit reason — no mystery drops.

[Open `src/db/schema.sql`.]

> Three clean tables with proper constraints — primary keys, foreign keys, NOT NULLs, CHECKs on email lowercase and status enum. Three quarantine tables with the same columns as TEXT, so we never fail to ingest a bad row just because it doesn't fit the type.

[Open `src/db/views.sql`.]

> Eight analytics views. `v_daily_metrics` for orders per day, revenue, and average order value. `v_top_customers` and `v_top_skus` use the `RANK` window function. Then five data-quality views including `v_quarantine_summary` which groups quarantine counts by table and reason — that becomes a dashboard signal in production.

[Open `src/etl/load.py`, scroll to the `copy()` block.]

> Load uses psycopg v3's `cursor.copy()` with `FORMAT TEXT, NULL ''` — that's 10 to 100 times faster than batched INSERTs. The `_clean_row` helper converts pandas NaN to empty strings so Postgres reads them as nulls. Everything is wrapped in a single transaction — clean and quarantine load together or not at all.

---

## SEGMENT 3 — Live Demo (~2 min)

[Switch to terminal. NO SCRIPT NEEDED — just narrate as you go. Practice this twice before recording.]

> Let me show this running. I'll start from a clean database.

```bash
python main.py truncate --yes
```

> All six tables wiped. Now I'll re-apply the schema and views.

```bash
python main.py init
```

> Schema and 8 views applied in under a second. Now the actual ETL.

```bash
python main.py run
```

> And there it is. Customers: 6 rows in, 4 loaded, 2 quarantined — one for invalid email format, one for duplicate email. Orders: 10 in, 6 loaded, 4 quarantined — one invalid status, three with missing customer FKs. Those three are the *cascade* — the bad customer was rejected upstream, so their orders can't be loaded either. And finally order items, where the cascade continues: orphan-order line items get quarantined too.

[Open REPORT.md in VS Code.]

> And the report. Total revenue R856 across 4 days. Top customer is Jane Doe at R501. Top SKU is A-001, also R501. And every quarantine reason is broken out at the bottom — full transparency on what was rejected and why.
>
> By the way — if `GROQ_API_KEY` is set, this same command produces an AI-written narrative summary using Llama 3.3 70B via Groq. The fallback to template mode means the feature is reviewer-friendly: it always works, with or without the key.

---

## SEGMENT 4 — Wrap + Trade-offs (~60s)

[Open SOLUTION.md briefly, then back to README.]

> Quick word on what I'd add for production. This is a single-shot batch CLI — in production I'd wrap it in cron or Airflow, push secrets to Vault instead of env vars, alert on non-zero exit and on high quarantine rates, and emit metrics to Prometheus. I deliberately scoped those out because they're operational concerns, not pipeline-design concerns. They're noted in SOLUTION.md.
>
> Test coverage is 35 unit tests, all passing, no database required — they run on pure DataFrames so they're fast and CI-friendly.
>
> Everything is on GitHub at github.com/Lebogang224/orders-pipeline. The full design is in docs/HIGH_LEVEL_DESIGN and LOW_LEVEL_DESIGN, both as markdown and PDF. Trade-off rationale is in SOLUTION.md.
>
> Thanks Karin — looking forward to your feedback.

[Stop recording.]

---

## DELIVERY REMINDERS

- **First line slow.** "Hi Karin, I'm Lebogang Mphaga..." — calm pace sets the whole video's tone.
- **Pause before key words.** "This is the... *quarantine pattern*."
- **Smile during intro and outro** — physically changes voice.
- **Glance at script, look at screen, speak.** Don't read word-for-word.
- **If you fumble:** pause 2 seconds, restate cleanly. Trim later in Loom.
- **Segments can be re-recorded individually** — pause and resume in Loom.

## NUMBERS TO REMEMBER (in case you blank)

| Thing | Number |
|---|---|
| Customers | 6 in → 4 loaded, 2 quarantined |
| Orders | 10 in → 6 loaded, 4 quarantined |
| Order items | 12 in → 5 loaded, 7 quarantined |
| Total revenue | R856 |
| Top customer | Jane Doe @ R501 |
| Top SKU | A-001 @ R501 |
| Tests | 35 passing |
| Views | 8 (3 analytics + 5 DQ) |
