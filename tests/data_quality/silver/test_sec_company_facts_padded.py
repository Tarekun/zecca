import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

lf = SecCompanyFactsPaddedSilver().read_from_disk()
facts_lf = SecCompanyFactsSilver().read_from_disk()


def test_no_null_cik():
    """No row in sec_company_facts_padded should have a null CIK."""

    null_rows = lf.select("cik").filter(pl.col("cik").is_null()).collect()

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null CIK.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_ticker():
    """No row in sec_company_facts_padded should have a null ticker.

    CIKs with no matching ticker are written to
    dataplatform/test_outputs/sec_company_facts_padded_missing_ticker.csv.
    """
    null_rows = (
        lf.select(["cik", "ticker"]).filter(pl.col("ticker").is_null()).collect()
    )

    if null_rows.height > 0:
        missing_ciks = null_rows.select("cik").unique().sort("cik")
        TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        missing_ciks.write_csv(
            TEST_OUTPUTS / "sec_company_facts_padded_missing_ticker.csv"
        )

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
    dates_lf = lf.select(["cik", "reference_date"])
    bounds = dates_lf.group_by("cik").agg(
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
    actual = dates_lf.unique()
    missing = expected.join(actual, on=["cik", "reference_date"], how="anti").collect()

    if missing.height > 0:
        counts = (
            missing.group_by("cik")
            .agg(pl.len().alias("missing_dates"))
            .sort("missing_dates", descending=True)
        )

        TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        counts.write_csv(TEST_OUTPUTS / "sec_company_facts_padded_date_gaps.csv")

        affected = counts["cik"].drop_nulls().to_list()
        pytest.fail(
            f"{missing.height} (cik, reference_date) pairs are missing after each CIK's first appearance.\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Gap counts written to dataplatform/test_outputs/sec_company_facts_padded_date_gaps.csv\n"
            f"Sample (up to 20):\n{missing.sort(['cik', 'reference_date']).head(20)}"
        )


def test_every_filing_end_date_present_as_reference_date():
    """Every (cik, end_date) pair for each metric in sec_company_facts must
    appear as a (cik, reference_date) row in sec_company_facts_padded — padding
    only fills the gaps between filings, it must never drop a filing's own date.

    Missing (cik, end_date, metric) triples are written to
    dataplatform/test_outputs/sec_company_facts_padded_missing_filing_dates.csv.
    """
    reference_dates = lf.select(["cik", "reference_date"]).unique()

    missing_frames = []
    for end_col in ["shares_outstanding_end", "public_float_end", "earnings_end"]:
        filings = (
            facts_lf.select(["cik", end_col])
            .filter(pl.col("cik").is_not_null() & pl.col(end_col).is_not_null())
            .unique()
            .rename({end_col: "reference_date"})
        )
        missing = (
            filings.join(reference_dates, on=["cik", "reference_date"], how="anti")
            .with_columns(pl.lit(end_col).alias("metric"))
            .collect()
        )
        if missing.height > 0:
            missing_frames.append(missing)

    if missing_frames:
        combined_missing = pl.concat(missing_frames).sort(
            ["metric", "cik", "reference_date"]
        )
        TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        combined_missing.write_csv(
            TEST_OUTPUTS / "sec_company_facts_padded_missing_filing_dates.csv"
        )

        affected = combined_missing.select("cik").unique().to_series().to_list()
        pytest.fail(
            f"{combined_missing.height} (cik, end_date) filing pairs from "
            f"sec_company_facts are missing from sec_company_facts_padded's reference_date.\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/sec_company_facts_padded_missing_filing_dates.csv"
        )
