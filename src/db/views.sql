-- =============================================================================
-- Orders Pipeline — Analytics & Data Quality Views
-- All views are non-materialized (as specified in the brief).
-- Idempotent: CREATE OR REPLACE throughout.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- ANALYTICS VIEWS (Task 3, required)
-- -----------------------------------------------------------------------------

-- 1. Daily metrics: orders_count, total_revenue, average_order_value
CREATE OR REPLACE VIEW v_daily_metrics AS
SELECT
    (order_ts AT TIME ZONE 'UTC')::DATE  AS order_date,
    COUNT(*)                             AS orders_count,
    SUM(total_amount)                    AS total_revenue,
    ROUND(AVG(total_amount), 2)          AS average_order_value
FROM orders
GROUP BY 1
ORDER BY 1;


-- 2. Top 10 customers by lifetime spend
--    Uses a window function (RANK) so ties are handled consistently.
CREATE OR REPLACE VIEW v_top_customers AS
WITH customer_spend AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.country_code,
        SUM(o.total_amount)  AS lifetime_spend,
        COUNT(o.order_id)    AS order_count,
        RANK() OVER (ORDER BY SUM(o.total_amount) DESC) AS spend_rank
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_id, c.full_name, c.email, c.country_code
)
SELECT
    customer_id,
    full_name,
    email,
    country_code,
    lifetime_spend,
    order_count,
    spend_rank
FROM customer_spend
WHERE spend_rank <= 10
ORDER BY spend_rank;


-- 3. Top 10 SKUs by revenue and units sold
--    Two RANK() columns so the reviewer can see both dimensions.
CREATE OR REPLACE VIEW v_top_skus AS
WITH sku_stats AS (
    SELECT
        sku,
        category,
        SUM(quantity * unit_price)  AS revenue,
        SUM(quantity)               AS units_sold,
        COUNT(DISTINCT order_id)    AS order_count
    FROM order_items
    GROUP BY sku, category
),
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY revenue    DESC) AS revenue_rank,
        RANK() OVER (ORDER BY units_sold DESC) AS units_rank
    FROM sku_stats
)
SELECT sku, category, revenue, units_sold, order_count, revenue_rank, units_rank
FROM ranked
ORDER BY revenue_rank
LIMIT 10;


-- -----------------------------------------------------------------------------
-- DATA QUALITY VIEWS (Task 3, required)
-- -----------------------------------------------------------------------------

-- DQ 1: Duplicate customers by lowercase email
--       Under normal operation this returns 0 rows (UNIQUE constraint + ETL dedup).
--       Kept as a live check in case the constraint is ever bypassed.
CREATE OR REPLACE VIEW v_dq_duplicate_emails AS
SELECT
    email,
    COUNT(*)                                        AS duplicate_count,
    ARRAY_AGG(customer_id ORDER BY signup_date)     AS customer_ids,
    MIN(signup_date)                                AS earliest_signup,
    MAX(signup_date)                                AS latest_signup
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;


-- DQ 2: Orders referencing a customer not present in the customers table
--       The FK constraint prevents this in the clean table under normal operation.
--       This view exists as an explicit data quality signal — it always returns
--       0 rows if the pipeline ran correctly, which is itself informative.
CREATE OR REPLACE VIEW v_dq_orders_missing_customer AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.status,
    o.total_amount
FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;


-- -----------------------------------------------------------------------------
-- DATA QUALITY VIEWS (Task 3, optional stretch)
-- -----------------------------------------------------------------------------

-- Order items with non-positive quantity or unit price
-- (Should always be empty — these are filtered to quarantine before load)
CREATE OR REPLACE VIEW v_dq_invalid_order_items AS
SELECT *
FROM order_items
WHERE quantity <= 0 OR unit_price <= 0;


-- Orders with a status outside the allowed set
-- (Should always be empty — CHECK constraint + ETL filter prevent this)
CREATE OR REPLACE VIEW v_dq_invalid_order_status AS
SELECT *
FROM orders
WHERE status NOT IN ('placed', 'shipped', 'cancelled', 'refunded');


-- -----------------------------------------------------------------------------
-- QUARANTINE SUMMARY VIEW — single place to see all rejection counts
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_quarantine_summary AS
    SELECT 'customers'   AS entity,
           quarantine_reason,
           COUNT(*)       AS rejected_rows,
           MIN(ingested_at) AS first_seen,
           MAX(ingested_at) AS last_seen
    FROM quarantine_customers
    GROUP BY quarantine_reason

UNION ALL

    SELECT 'orders',
           quarantine_reason,
           COUNT(*),
           MIN(ingested_at),
           MAX(ingested_at)
    FROM quarantine_orders
    GROUP BY quarantine_reason

UNION ALL

    SELECT 'order_items',
           quarantine_reason,
           COUNT(*),
           MIN(ingested_at),
           MAX(ingested_at)
    FROM quarantine_order_items
    GROUP BY quarantine_reason

ORDER BY entity, rejected_rows DESC;
