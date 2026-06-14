import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

_PROJECT_ROOT = Path(__file__).parents[3]
_SILVER_PARQUET = (
    _PROJECT_ROOT
    / "dataplatform"
    / "silver"
    / "sec_company_facts_padded"
    / "sec_company_facts_padded.parquet"
)
_TEST_OUTPUTS = _PROJECT_ROOT / "dataplatform" / "test_outputs"


def test_no_null_cik():
    """No row in sec_company_facts_padded should have a null CIK."""
    df = pl.read_parquet(_SILVER_PARQUET)
    null_rows = df.filter(pl.col("cik").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null CIK.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_reference_date_continuity_per_cik():
    """For every CIK the reference_date column must be continuous — no gaps between
    the first and last date observed for that CIK.

    CIKs with gaps are written to
    dataplatform/test_outputs/sec_company_facts_padded_date_gaps.csv.
    """
    df = pl.read_parquet(_SILVER_PARQUET)

    bounds = df.group_by("cik").agg(
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

    actual = df.select(["cik", "reference_date"]).unique()

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
