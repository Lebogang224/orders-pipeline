-- =============================================================================
-- Orders Pipeline — Schema
-- Idempotent: safe to run multiple times (IF NOT EXISTS throughout)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- PRIMARY TABLES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS customers (
    customer_id   INTEGER      NOT NULL,
    email         TEXT         NOT NULL,
    full_name     TEXT         NOT NULL,
    signup_date   DATE         NOT NULL,
    country_code  CHAR(2),                   -- nullable: not all records have it
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,

    CONSTRAINT customers_pk
        PRIMARY KEY (customer_id),

    -- Store and enforce lowercase at the DB level (no citext extension needed)
    CONSTRAINT customers_email_lowercase_chk
        CHECK (email = lower(email)),

    -- Unique on the stored (already lowercase) value
    CONSTRAINT customers_email_unique
        UNIQUE (email)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGINT                    NOT NULL,
    customer_id   INTEGER                   NOT NULL,
    order_ts      TIMESTAMP WITH TIME ZONE  NOT NULL,
    status        TEXT                      NOT NULL,
    total_amount  NUMERIC(12, 2)            NOT NULL,
    currency      CHAR(3)                   NOT NULL,

    CONSTRAINT orders_pk
        PRIMARY KEY (order_id),

    CONSTRAINT orders_customer_fk
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),

    -- TEXT + CHECK is easier to evolve than a PostgreSQL ENUM
    -- (adding a new status value is a 1-line CHECK change, vs ALTER TYPE)
    CONSTRAINT orders_status_chk
        CHECK (status IN ('placed', 'shipped', 'cancelled', 'refunded'))
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id    BIGINT          NOT NULL,
    line_no     INTEGER         NOT NULL,
    sku         TEXT            NOT NULL,
    quantity    INTEGER         NOT NULL,
    unit_price  NUMERIC(12, 2)  NOT NULL,
    category    TEXT,

    CONSTRAINT order_items_pk
        PRIMARY KEY (order_id, line_no),

    CONSTRAINT order_items_order_fk
        FOREIGN KEY (order_id) REFERENCES orders (order_id),

    -- Enforce positive values at the DB level as a last line of defence
    -- (ETL layer already filters these out, but defence-in-depth matters)
    CONSTRAINT order_items_qty_positive_chk
        CHECK (quantity > 0),

    CONSTRAINT order_items_price_positive_chk
        CHECK (unit_price > 0)
);

-- -----------------------------------------------------------------------------
-- QUARANTINE TABLES
-- All columns stored as TEXT to preserve exactly what arrived from the source.
-- No foreign keys by design — these rows violated referential integrity.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quarantine_customers (
    customer_id       TEXT,
    email             TEXT,
    full_name         TEXT,
    signup_date       TEXT,
    country_code      TEXT,
    is_active         TEXT,
    quarantine_reason TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quarantine_orders (
    order_id          TEXT,
    customer_id       TEXT,
    order_ts          TEXT,
    status            TEXT,
    total_amount      TEXT,
    currency          TEXT,
    quarantine_reason TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quarantine_order_items (
    order_id          TEXT,
    line_no           TEXT,
    sku               TEXT,
    quantity          TEXT,
    unit_price        TEXT,
    category          TEXT,
    quarantine_reason TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    ingested_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- INDEXES — lean set, only where views or joins need them
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_order_ts
    ON orders (order_ts);

CREATE INDEX IF NOT EXISTS idx_order_items_sku
    ON order_items (sku);
