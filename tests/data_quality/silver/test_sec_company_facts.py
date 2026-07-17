import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver

PROJECT_ROOT = Path(__file__).parents[3]
RAW_SEC_DIR = PROJECT_ROOT / "dataplatform" / "raw" / "sec"
TEST_OUTPUTS = PROJECT_ROOT / "dataplatform" / "test_outputs"

lf = SecCompanyFactsSilver().read_from_disk()


def test_no_null_cik():
    """No row in sec_company_facts should have a null CIK."""

    null_rows = lf.select("cik").filter(pl.col("cik").is_null()).collect()

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null CIK.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_cik_count_matches_file_count():
    """The number of distinct CIK values in the silver model must equal the number
    of source JSON files under dataplatform/raw/sec — one row (possibly null) per file.
    """
    file_count = len(list(RAW_SEC_DIR.glob("*.json")))
    distinct_ciks = lf.select(pl.col("cik").n_unique()).collect().item()

    assert distinct_ciks == file_count, (
        f"Expected {file_count} distinct CIK values (one per source file) "
        f"but found {distinct_ciks}."
    )


def test_each_cik_has_at_least_one_metric():
    """Every CIK in the silver model must have at least one row with a non-null
    shares_outstanding or non_affiliate_valuation.

    CIKs with no metric data are written to
    dataplatform/test_outputs/sec_company_facts_missing_val.csv for inspection.
    """
    metrics_lf = lf.select(["cik", "shares_outstanding", "non_affiliate_valuation"])
    ciks_with_data = (
        metrics_lf.filter(
            pl.col("shares_outstanding").is_not_null()
            | pl.col("non_affiliate_valuation").is_not_null()
        )
        .select("cik")
        .unique()
    )
    all_ciks = metrics_lf.select("cik").unique()

    missing_val = (
        all_ciks.join(ciks_with_data, on="cik", how="anti").sort("cik").collect()
    )

    if missing_val.height > 0:
        TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        missing_val.write_csv(TEST_OUTPUTS / "sec_company_facts_missing_val.csv")

        affected = missing_val["cik"].drop_nulls().to_list()
        pytest.fail(
            f"{missing_val.height} CIK(s) have no rows with a non-null shares_outstanding or non_affiliate_valuation.\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/sec_company_facts_missing_val.csv"
        )
