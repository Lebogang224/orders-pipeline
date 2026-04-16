"""
Quarantine helpers.

A quarantine row = the original source row (all TEXT) + audit columns:
  - quarantine_reason: machine-readable string e.g. "invalid_email_format"
  - source_file:       filename the row came from
  - ingested_at:       set by DB DEFAULT NOW()

Design decision: we split a DataFrame into (clean, quarantine) pairs
rather than dropping bad rows. This preserves data for investigation
and gives the data team visibility into rejection rates.
"""
from typing import Tuple

import pandas as pd


def split(
    df: pd.DataFrame,
    mask: pd.Series,
    reason: str,
    source_file: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split df into (clean_rows, quarantine_rows) based on a boolean mask.

    Args:
        df:          DataFrame to split
        mask:        True  = row is VALID (keep in clean)
                     False = row is BAD (send to quarantine)
        reason:      quarantine_reason string for bad rows
        source_file: source filename for audit trail

    Returns:
        (clean_df, quarantine_df)
        quarantine_df has all original columns (as str) + reason + source_file
    """
    clean = df[mask].copy()
    bad   = df[~mask].copy()

    if not bad.empty:
        # Coerce all columns to str to match quarantine table schema
        bad = bad.astype(str)
        bad["quarantine_reason"] = reason
        bad["source_file"]       = source_file

    return clean, bad


def add_reason(df: pd.DataFrame, reason: str, source_file: str) -> pd.DataFrame:
    """
    Mark an entire DataFrame as quarantined (every row gets the reason).
    Used when an entire batch fails a check (e.g. all rows of a file are bad).
    """
    if df.empty:
        return df
    out = df.astype(str).copy()
    out["quarantine_reason"] = reason
    out["source_file"]       = source_file
    return out
