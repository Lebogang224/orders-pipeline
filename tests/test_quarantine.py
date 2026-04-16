"""
Unit tests for the quarantine helper.

Verifies that split() correctly separates clean from bad rows
and attaches the right audit columns.
"""
import pytest
import pandas as pd

from src.etl.quarantine import split, add_reason


class TestSplit:

    def _make_df(self):
        return pd.DataFrame({
            "id":    [1, 2, 3],
            "value": ["good", "bad", "good"],
        })

    def test_clean_rows_returned_correctly(self):
        df = self._make_df()
        mask = df["value"] == "good"
        clean, q = split(df, mask, "bad_value", "test.csv")
        assert len(clean) == 2
        assert list(clean["id"]) == [1, 3]

    def test_quarantine_rows_returned_correctly(self):
        df = self._make_df()
        mask = df["value"] == "good"
        clean, q = split(df, mask, "bad_value", "test.csv")
        assert len(q) == 1
        assert q.iloc[0]["id"] == "2"  # coerced to str

    def test_quarantine_reason_attached(self):
        df = self._make_df()
        mask = df["value"] == "good"
        _, q = split(df, mask, "my_reason", "test.csv")
        assert q.iloc[0]["quarantine_reason"] == "my_reason"

    def test_source_file_attached(self):
        df = self._make_df()
        mask = df["value"] == "good"
        _, q = split(df, mask, "reason", "customers.csv")
        assert q.iloc[0]["source_file"] == "customers.csv"

    def test_all_rows_valid_returns_empty_quarantine(self):
        df = self._make_df()
        mask = pd.Series([True, True, True])
        clean, q = split(df, mask, "reason", "test.csv")
        assert len(clean) == 3
        assert q.empty

    def test_all_rows_invalid_returns_empty_clean(self):
        df = self._make_df()
        mask = pd.Series([False, False, False])
        clean, q = split(df, mask, "reason", "test.csv")
        assert clean.empty
        assert len(q) == 3

    def test_quarantine_values_coerced_to_str(self):
        """Quarantine tables use TEXT columns — all values must be strings."""
        df = pd.DataFrame({"id": [1], "amount": [99.99], "active": [True]})
        mask = pd.Series([False])
        _, q = split(df, mask, "reason", "test.csv")
        for col in ["id", "amount", "active"]:
            assert isinstance(q.iloc[0][col], str), f"{col} should be str"

    def test_original_df_not_mutated(self):
        df = self._make_df()
        original_cols = list(df.columns)
        mask = df["value"] == "good"
        split(df, mask, "reason", "test.csv")
        assert list(df.columns) == original_cols


class TestAddReason:

    def test_adds_reason_to_all_rows(self):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        out = add_reason(df, "bulk_reject", "orders.jsonl")
        assert len(out) == 2
        assert all(out["quarantine_reason"] == "bulk_reject")
        assert all(out["source_file"] == "orders.jsonl")

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame()
        out = add_reason(df, "reason", "file.csv")
        assert out.empty
