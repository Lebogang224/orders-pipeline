"""
Unit tests for the transform layer.

Tests run on pure DataFrames — no database connection needed.
Each test covers one specific validation rule so failures are easy to diagnose.
"""
import datetime
import pytest
import pandas as pd

from src.config import load_config
from src.etl.transform import (
    transform_customers,
    transform_orders,
    transform_order_items,
)

cfg = load_config()


# =============================================================================
# HELPERS
# =============================================================================

def make_customers(**overrides) -> pd.DataFrame:
    """Return a single-row customers DataFrame with sane defaults."""
    row = {
        "customer_id": "1",
        "email": "valid@example.com",
        "full_name": "Test User",
        "signup_date": "2024-01-01",
        "country_code": "ZA",
        "is_active": "true",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def make_orders(**overrides) -> pd.DataFrame:
    row = {
        "order_id": "1001",
        "customer_id": "1",
        "order_ts": "2024-03-01T08:00:00+02:00",
        "status": "placed",
        "total_amount": "100.00",
        "currency": "ZAR",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def make_items(**overrides) -> pd.DataFrame:
    row = {
        "order_id": "1001",
        "line_no": "1",
        "sku": "A-001",
        "quantity": "2",
        "unit_price": "50.00",
        "category": "Electronics",
    }
    row.update(overrides)
    return pd.DataFrame([row])


# =============================================================================
# CUSTOMER TESTS
# =============================================================================

class TestTransformCustomers:

    def test_valid_row_passes(self):
        clean, q = transform_customers(make_customers(), cfg)
        assert len(clean) == 1
        assert len(q) == 0

    def test_email_normalised_to_lowercase(self):
        clean, _ = transform_customers(make_customers(email="UPPER@EXAMPLE.COM"), cfg)
        assert clean.iloc[0]["email"] == "upper@example.com"

    def test_email_whitespace_stripped(self):
        clean, _ = transform_customers(make_customers(email="  valid@example.com  "), cfg)
        assert clean.iloc[0]["email"] == "valid@example.com"

    def test_invalid_email_quarantined(self):
        clean, q = transform_customers(make_customers(email="bademail"), cfg)
        assert len(clean) == 0
        assert len(q) == 1
        assert q.iloc[0]["quarantine_reason"] == "invalid_email_format"

    def test_email_missing_at_quarantined(self):
        clean, q = transform_customers(make_customers(email="nodomain"), cfg)
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "invalid_email_format"

    def test_invalid_signup_date_quarantined(self):
        clean, q = transform_customers(make_customers(signup_date="not-a-date"), cfg)
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "invalid_signup_date"

    def test_duplicate_email_keeps_earliest(self):
        df = pd.DataFrame([
            {"customer_id": "1", "email": "dup@example.com", "full_name": "Early",
             "signup_date": "2024-01-01", "country_code": "ZA", "is_active": "true"},
            {"customer_id": "2", "email": "dup@example.com", "full_name": "Late",
             "signup_date": "2024-06-01", "country_code": "ZA", "is_active": "true"},
        ])
        clean, q = transform_customers(df, cfg)
        assert len(clean) == 1
        assert clean.iloc[0]["full_name"] == "Early"
        assert len(q) == 1
        assert q.iloc[0]["quarantine_reason"] == "duplicate_email"

    def test_duplicate_email_case_insensitive(self):
        """JOHN@EXAMPLE.COM and john@example.com are the same after normalisation."""
        df = pd.DataFrame([
            {"customer_id": "1", "email": "JOHN@EXAMPLE.COM", "full_name": "Upper",
             "signup_date": "2024-01-01", "country_code": "ZA", "is_active": "true"},
            {"customer_id": "2", "email": "john@example.com", "full_name": "Lower",
             "signup_date": "2024-06-01", "country_code": "ZA", "is_active": "true"},
        ])
        clean, q = transform_customers(df, cfg)
        assert len(clean) == 1
        assert len(q) == 1

    def test_null_country_code_allowed_by_default(self):
        clean, q = transform_customers(make_customers(country_code=""), cfg)
        assert len(clean) == 1
        # pandas stores None as NaN in object columns; pd.isna covers both
        assert pd.isna(clean.iloc[0]["country_code"])

    def test_is_active_cast_to_bool(self):
        clean, _ = transform_customers(make_customers(is_active="false"), cfg)
        # numpy.bool_(False) == False but numpy.bool_(False) is not False
        assert clean.iloc[0]["is_active"] == False  # noqa: E712


# =============================================================================
# ORDER TESTS
# =============================================================================

class TestTransformOrders:

    VALID_CUSTOMERS = {1}

    def test_valid_row_passes(self):
        clean, q = transform_orders(make_orders(), self.VALID_CUSTOMERS, cfg)
        assert len(clean) == 1
        assert len(q) == 0

    def test_invalid_status_quarantined(self):
        clean, q = transform_orders(
            make_orders(status="processing"), self.VALID_CUSTOMERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "invalid_status"

    def test_all_valid_statuses_pass(self):
        for status in ("placed", "shipped", "cancelled", "refunded"):
            clean, q = transform_orders(make_orders(status=status), self.VALID_CUSTOMERS, cfg)
            assert len(clean) == 1, f"Status '{status}' should be valid"

    def test_unknown_customer_fk_quarantined(self):
        clean, q = transform_orders(make_orders(customer_id="999"), self.VALID_CUSTOMERS, cfg)
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "missing_customer_fk"

    def test_iso8601_with_timezone_parsed(self):
        clean, _ = transform_orders(
            make_orders(order_ts="2024-03-01T08:12:00+02:00"), self.VALID_CUSTOMERS, cfg
        )
        assert clean.iloc[0]["order_ts"] is not None

    def test_space_separated_datetime_parsed(self):
        clean, q = transform_orders(
            make_orders(order_ts="2024-03-03 11:30:00"), self.VALID_CUSTOMERS, cfg
        )
        assert len(clean) == 1

    def test_slash_datetime_parsed(self):
        clean, q = transform_orders(
            make_orders(order_ts="2024/03/04 12:00:00"), self.VALID_CUSTOMERS, cfg
        )
        assert len(clean) == 1

    def test_unparseable_timestamp_quarantined(self):
        clean, q = transform_orders(
            make_orders(order_ts="not-a-date"), self.VALID_CUSTOMERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "invalid_order_ts"

    def test_timestamps_converted_to_utc(self):
        """Both +02:00 and Z timestamps should end up as UTC."""
        df = pd.concat([
            make_orders(order_id="1001", order_ts="2024-03-01T08:00:00+02:00"),
            make_orders(order_id="1002", order_ts="2024-03-01T06:00:00Z"),
        ])
        clean, _ = transform_orders(df, {1}, cfg)
        ts1 = clean[clean["order_id"] == 1001].iloc[0]["order_ts"]
        ts2 = clean[clean["order_id"] == 1002].iloc[0]["order_ts"]
        # Both should be the same UTC moment (06:00 UTC = 08:00 SAST)
        assert ts1 == ts2


# =============================================================================
# ORDER ITEMS TESTS
# =============================================================================

class TestTransformOrderItems:

    VALID_ORDERS = {1001}

    def test_valid_row_passes(self):
        clean, q = transform_order_items(make_items(), self.VALID_ORDERS, cfg)
        assert len(clean) == 1
        assert len(q) == 0

    def test_zero_quantity_quarantined(self):
        clean, q = transform_order_items(
            make_items(quantity="0"), self.VALID_ORDERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "non_positive_quantity"

    def test_negative_quantity_quarantined(self):
        clean, q = transform_order_items(
            make_items(quantity="-1"), self.VALID_ORDERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "non_positive_quantity"

    def test_zero_unit_price_quarantined(self):
        clean, q = transform_order_items(
            make_items(unit_price="0.00"), self.VALID_ORDERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "non_positive_unit_price"

    def test_unknown_order_fk_quarantined(self):
        clean, q = transform_order_items(
            make_items(order_id="9999"), self.VALID_ORDERS, cfg
        )
        assert len(clean) == 0
        assert q.iloc[0]["quarantine_reason"] == "missing_order_fk"

    def test_positive_values_pass(self):
        clean, q = transform_order_items(
            make_items(quantity="1", unit_price="0.01"), self.VALID_ORDERS, cfg
        )
        assert len(clean) == 1
        assert len(q) == 0
