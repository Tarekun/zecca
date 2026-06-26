"""Unit tests for cross-metric padding isolation in sec_company_facts_padded.

The two metrics (EntityCommonStockSharesOutstanding and EntityPublicFloat) are
independent time series with different reporting cadences.  These tests verify
that the forward-fill boundary of each metric is driven exclusively by its own
next entry, never by an entry from the other metric.
"""
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts_padded import _pad_series

_TODAY = date(2024, 1, 1)


def _shares_df(cik: int, ends: list[date], values: list[int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cik": pl.Series([cik] * len(ends), dtype=pl.Int64),
            "shares_outstanding_end": ends,
            "shares_outstanding_fp": ["FY"] * len(ends),
            "shares_outstanding": pl.Series(values, dtype=pl.Int64),
        }
    )


def _float_df(cik: int, ends: list[date], values: list[int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cik": pl.Series([cik] * len(ends), dtype=pl.Int64),
            "public_float_end": ends,
            "non_affiliate_valuation": pl.Series(values, dtype=pl.Int64),
        }
    )


def test_shares_padding_boundary_ignores_public_float_dates():
    """Shares padding must roll forward to the next *shares* entry, not to the
    next public_float entry even when the float date falls between two share dates."""
    # shares: 2020-01-01 (100 shares) → 2021-01-01 (200 shares)
    # float:  2020-06-30 falls between the two share entries
    # Expected: shares=100 throughout 2020-01-01..2020-12-31; shares=200 from 2021-01-01
    shares = _shares_df(1, [date(2020, 1, 1), date(2021, 1, 1)], [100, 200])
    padded = _pad_series(shares, end_col="shares_outstanding_end", today=_TODAY)

    # The float's end date should not create a boundary in the shares series
    assert padded.filter(pl.col("reference_date") == date(2020, 6, 30))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2020, 12, 31))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2021, 1, 1))["shares_outstanding"][0] == 200


def test_float_padding_boundary_ignores_shares_dates():
    """Public-float padding must roll forward to the next *float* entry, not to
    the next shares entry even when the shares date falls between two float dates."""
    # float:  2020-12-31 (1M) → 2021-12-31 (2M)
    # shares: 2021-06-01 falls between the two float entries
    # Expected: non_affiliate_valuation=1M throughout 2020-12-31..2021-12-30
    floats = _float_df(1, [date(2020, 12, 31), date(2021, 12, 31)], [1_000_000, 2_000_000])
    padded = _pad_series(floats, end_col="public_float_end", today=_TODAY)

    assert padded.filter(pl.col("reference_date") == date(2021, 6, 1))["non_affiliate_valuation"][0] == 1_000_000
    assert padded.filter(pl.col("reference_date") == date(2021, 12, 30))["non_affiliate_valuation"][0] == 1_000_000
    assert padded.filter(pl.col("reference_date") == date(2021, 12, 31))["non_affiliate_valuation"][0] == 2_000_000


def test_combined_join_preserves_independent_boundaries():
    """After joining the two padded series, values at every reference_date reflect
    each metric's own most-recent entry, with no cross-contamination."""
    # CIK 1:
    #   shares: 2020-01-01 (100) → 2021-01-01 (200)
    #   float:  2020-06-30 (1M)  → 2021-06-30 (2M)
    shares = _shares_df(1, [date(2020, 1, 1), date(2021, 1, 1)], [100, 200])
    floats = _float_df(1, [date(2020, 6, 30), date(2021, 6, 30)], [1_000_000, 2_000_000])

    shares_padded = _pad_series(shares, end_col="shares_outstanding_end", today=_TODAY)
    float_padded = _pad_series(floats, end_col="public_float_end", today=_TODAY)

    combined = shares_padded.join(
        float_padded,
        on=["cik", "reference_date"],
        how="full",
        coalesce=True,
    )

    def row(ref: date) -> dict:
        r = combined.filter(pl.col("reference_date") == ref)
        return {
            "shares": r["shares_outstanding"][0],
            "float": r["non_affiliate_valuation"][0],
        }

    # Before float starts: shares present, float null
    assert row(date(2020, 3, 15)) == {"shares": 100, "float": None}

    # After float starts but before next shares entry: both present, shares unchanged
    assert row(date(2020, 9, 1)) == {"shares": 100, "float": 1_000_000}

    # After new shares entry but before new float entry: shares updated, float unchanged
    assert row(date(2021, 3, 1)) == {"shares": 200, "float": 1_000_000}

    # After new float entry: both updated
    assert row(date(2021, 9, 1)) == {"shares": 200, "float": 2_000_000}


def test_multiple_ciks_do_not_cross_pad():
    """Padding per CIK is strictly isolated: a later entry for CIK B must not
    truncate the forward-fill range of CIK A."""
    # CIK 1: one shares entry at 2020-01-01 (value 50)
    # CIK 2: shares entry at 2020-06-01 (value 99)
    # CIK 1 should be padded to _TODAY - 1, unaffected by CIK 2's entry.
    shares = pl.concat([
        _shares_df(1, [date(2020, 1, 1)], [50]),
        _shares_df(2, [date(2020, 6, 1)], [99]),
    ])
    padded = _pad_series(shares, end_col="shares_outstanding_end", today=_TODAY)

    cik1 = padded.filter(pl.col("cik") == 1)
    assert cik1["reference_date"].max() == _TODAY  # padded through today inclusive
    assert cik1.filter(pl.col("reference_date") == date(2020, 9, 1))["shares_outstanding"][0] == 50


def test_null_end_rows_are_excluded_from_padding():
    """Rows with a null end date must be silently dropped; they must not be
    treated as entries and must not corrupt surrounding boundaries."""
    shares = pl.DataFrame(
        {
            "cik": pl.Series([1, 1, 1], dtype=pl.Int64),
            "shares_outstanding_end": [date(2020, 1, 1), None, date(2021, 1, 1)],
            "shares_outstanding_fp": ["FY", None, "FY"],
            "shares_outstanding": pl.Series([100, None, 200], dtype=pl.Int64),
        }
    )
    padded = _pad_series(shares, end_col="shares_outstanding_end", today=_TODAY)

    # Boundary should be 2020-12-31 (day before 2021-01-01), not disturbed by the null row
    assert padded.filter(pl.col("reference_date") == date(2020, 12, 31))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2021, 1, 1))["shares_outstanding"][0] == 200
