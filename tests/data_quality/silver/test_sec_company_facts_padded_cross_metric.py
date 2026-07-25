"""Unit tests for cross-metric padding isolation in sec_company_facts_padded.

The two metrics (EntityCommonStockSharesOutstanding and EntityPublicFloat) are
independent time series with different reporting cadences.  These tests verify
that the forward-fill boundary of each metric is driven exclusively by its own
next entry (by filing date), never by an entry from the other metric.
"""
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts_padded import _pad_series

_TODAY = date(2024, 1, 1)


def _shares_df(
    cik: int, ends: list[date], filed: list[date], values: list[int]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cik": pl.Series([cik] * len(ends), dtype=pl.Int64),
            "shares_outstanding_end": ends,
            "shares_outstanding_filed": filed,
            "shares_outstanding_fp": ["FY"] * len(ends),
            "shares_outstanding": pl.Series(values, dtype=pl.Int64),
        }
    )


def _float_df(
    cik: int, ends: list[date], filed: list[date], values: list[int]
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "cik": pl.Series([cik] * len(ends), dtype=pl.Int64),
            "public_float_end": ends,
            "public_float_filed": filed,
            "non_affiliate_valuation": pl.Series(values, dtype=pl.Int64),
        }
    )


def _pad_shares(df: pl.DataFrame) -> pl.LazyFrame:
    return _pad_series(
        df,
        end_col="shares_outstanding_end",
        filed_col="shares_outstanding_filed",
        today=_TODAY,
    )


def _pad_float(df: pl.DataFrame) -> pl.LazyFrame:
    return _pad_series(
        df,
        end_col="public_float_end",
        filed_col="public_float_filed",
        today=_TODAY,
    )


def test_reference_date_is_anchored_on_filed_not_end():
    """A value only becomes public once it's filed, so reference_date must start
    at shares_outstanding_filed — not at the (earlier) reported period end."""
    # Reported period ends 2020-01-01 but isn't filed until 2020-03-15.
    shares = _shares_df(1, [date(2020, 1, 1)], [date(2020, 3, 15)], [100])
    padded = _pad_shares(shares)

    # Before the filing date the value must not be visible yet (look-ahead bias).
    assert padded.filter(pl.col("reference_date") == date(2020, 2, 1)).height == 0
    assert padded.filter(pl.col("reference_date") == date(2020, 3, 15))["shares_outstanding"][0] == 100
    # end_col/filed_col are carried through unchanged for the active entry.
    row = padded.filter(pl.col("reference_date") == date(2020, 6, 1))
    assert row["shares_outstanding_end"][0] == date(2020, 1, 1)
    assert row["shares_outstanding_filed"][0] == date(2020, 3, 15)


def test_shares_padding_boundary_follows_its_own_filed_dates():
    """Shares padding must roll forward at the next *shares* filing date, not be
    influenced by any other metric's dates."""
    # shares: filed 2020-01-15 (100) -> filed 2021-02-01 (200)
    shares = _shares_df(
        1,
        [date(2020, 1, 1), date(2021, 1, 1)],
        [date(2020, 1, 15), date(2021, 2, 1)],
        [100, 200],
    )
    padded = _pad_shares(shares)

    assert padded.filter(pl.col("reference_date") == date(2020, 6, 30))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2021, 1, 31))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2021, 2, 1))["shares_outstanding"][0] == 200


def test_float_padding_boundary_follows_its_own_filed_dates():
    """Public-float padding must roll forward at the next *float* filing date,
    not be influenced by any other metric's dates."""
    # float: filed 2021-02-15 (1M) -> filed 2022-02-10 (2M)
    floats = _float_df(
        1,
        [date(2020, 12, 31), date(2021, 12, 31)],
        [date(2021, 2, 15), date(2022, 2, 10)],
        [1_000_000, 2_000_000],
    )
    padded = _pad_float(floats)

    assert padded.filter(pl.col("reference_date") == date(2021, 6, 1))["non_affiliate_valuation"][0] == 1_000_000
    assert padded.filter(pl.col("reference_date") == date(2022, 2, 9))["non_affiliate_valuation"][0] == 1_000_000
    assert padded.filter(pl.col("reference_date") == date(2022, 2, 10))["non_affiliate_valuation"][0] == 2_000_000


def test_combined_join_preserves_independent_boundaries():
    """After joining the two padded series, values at every reference_date reflect
    each metric's own most-recent filing, with no cross-contamination."""
    # CIK 1:
    #   shares: filed 2020-01-15 (100) -> filed 2021-01-20 (200)
    #   float:  filed 2020-08-10 (1M)  -> filed 2021-08-15 (2M)
    shares = _shares_df(
        1,
        [date(2020, 1, 1), date(2021, 1, 1)],
        [date(2020, 1, 15), date(2021, 1, 20)],
        [100, 200],
    )
    floats = _float_df(
        1,
        [date(2020, 6, 30), date(2021, 6, 30)],
        [date(2020, 8, 10), date(2021, 8, 15)],
        [1_000_000, 2_000_000],
    )

    shares_padded = _pad_shares(shares)
    float_padded = _pad_float(floats)

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

    # Before float is filed: shares present, float null
    assert row(date(2020, 3, 15)) == {"shares": 100, "float": None}

    # After float is filed but before next shares filing: both present, shares unchanged
    assert row(date(2020, 9, 1)) == {"shares": 100, "float": 1_000_000}

    # After new shares filing but before new float filing: shares updated, float unchanged
    assert row(date(2021, 3, 1)) == {"shares": 200, "float": 1_000_000}

    # After new float filing: both updated
    assert row(date(2021, 9, 1)) == {"shares": 200, "float": 2_000_000}


def test_multiple_ciks_do_not_cross_pad():
    """Padding per CIK is strictly isolated: a later entry for CIK B must not
    truncate the forward-fill range of CIK A."""
    # CIK 1: one shares entry filed 2020-01-10 (value 50)
    # CIK 2: shares entry filed 2020-06-10 (value 99)
    # CIK 1 should be padded to _TODAY, unaffected by CIK 2's filing.
    shares = pl.concat([
        _shares_df(1, [date(2020, 1, 1)], [date(2020, 1, 10)], [50]),
        _shares_df(2, [date(2020, 6, 1)], [date(2020, 6, 10)], [99]),
    ])
    padded = _pad_shares(shares)

    cik1 = padded.filter(pl.col("cik") == 1)
    assert cik1["reference_date"].max() == _TODAY  # padded through today inclusive
    assert cik1.filter(pl.col("reference_date") == date(2020, 9, 1))["shares_outstanding"][0] == 50


def test_null_end_or_filed_rows_are_excluded_from_padding():
    """Rows with a null end or filed date must be silently dropped; they must
    not be treated as entries and must not corrupt surrounding boundaries."""
    shares = pl.DataFrame(
        {
            "cik": pl.Series([1, 1, 1], dtype=pl.Int64),
            "shares_outstanding_end": [date(2020, 1, 1), None, date(2021, 1, 1)],
            "shares_outstanding_filed": [date(2020, 1, 15), None, date(2021, 1, 20)],
            "shares_outstanding_fp": ["FY", None, "FY"],
            "shares_outstanding": pl.Series([100, None, 200], dtype=pl.Int64),
        }
    )
    padded = _pad_shares(shares)

    # Boundary should be 2021-01-19 (day before the next filing), not disturbed by the null row
    assert padded.filter(pl.col("reference_date") == date(2021, 1, 19))["shares_outstanding"][0] == 100
    assert padded.filter(pl.col("reference_date") == date(2021, 1, 20))["shares_outstanding"][0] == 200
