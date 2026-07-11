import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts_padded import SecCompanyFactsPaddedSilver

_TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

_df = SecCompanyFactsPaddedSilver().read_from_disk().collect()


def test_no_null_cik():
    """No row in sec_company_facts_padded should have a null CIK."""
    null_rows = _df.filter(pl.col("cik").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null CIK.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_ticker():
    """No row in sec_company_facts_padded should have a null ticker.

    CIKs with no matching ticker are written to
    dataplatform/test_outputs/sec_company_facts_padded_missing_ticker.csv.
    """
    null_rows = _df.filter(pl.col("ticker").is_null())

    if null_rows.height > 0:
        missing_ciks = null_rows.select("cik").unique().sort("cik")
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        missing_ciks.write_csv(_TEST_OUTPUTS / "sec_company_facts_padded_missing_ticker.csv")

        affected = missing_ciks["cik"].drop_nulls().to_list()
        pytest.fail(
            f"Found {null_rows.height} row(s) with a null ticker across {len(affected)} CIK(s).\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/sec_company_facts_padded_missing_ticker.csv"
        )


def test_reference_date_continuity_per_cik():
    """For every CIK the reference_date column must be continuous — no gaps between
    the first and last date observed for that CIK.

    CIKs with gaps are written to
    dataplatform/test_outputs/sec_company_facts_padded_date_gaps.csv.
    """
    bounds = _df.group_by("cik").agg(
        pl.col("reference_date").min().alias("first_date"),
        pl.col("reference_date").max().alias("last_date"),
    )

    expected = (
        bounds.with_columns(
            pl.date_ranges(
                pl.col("first_date"), pl.col("last_date"), interval="1d"
            ).alias("reference_date")
        )
        .explode("reference_date")
        .select(["cik", "reference_date"])
    )

    actual = _df.select(["cik", "reference_date"]).unique()

    missing = expected.join(actual, on=["cik", "reference_date"], how="anti")

    if missing.height > 0:
        counts = (
            missing.group_by("cik")
            .agg(pl.len().alias("missing_dates"))
            .sort("missing_dates", descending=True)
        )

        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        counts.write_csv(_TEST_OUTPUTS / "sec_company_facts_padded_date_gaps.csv")

        affected = counts["cik"].drop_nulls().to_list()
        pytest.fail(
            f"{missing.height} (cik, reference_date) pairs are missing after each CIK's first appearance.\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Gap counts written to dataplatform/test_outputs/sec_company_facts_padded_date_gaps.csv\n"
            f"Sample (up to 20):\n{missing.sort(['cik', 'reference_date']).head(20)}"
        )
