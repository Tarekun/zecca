import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver

_PROJECT_ROOT = Path(__file__).parents[3]
_RAW_SEC_DIR = _PROJECT_ROOT / "dataplatform" / "raw" / "sec"
_TEST_OUTPUTS = _PROJECT_ROOT / "dataplatform" / "test_outputs"

_df = SecCompanyFactsSilver().load_from_disk()


def test_no_null_cik():
    """No row in sec_company_facts should have a null CIK."""
    null_rows = _df.filter(pl.col("cik").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null CIK.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_cik_count_matches_file_count():
    """The number of distinct CIK values in the silver model must equal the number
    of source JSON files under dataplatform/raw/sec — one row (possibly null) per file."""
    file_count = len(list(_RAW_SEC_DIR.glob("*.json")))
    distinct_ciks = _df["cik"].n_unique()

    assert distinct_ciks == file_count, (
        f"Expected {file_count} distinct CIK values (one per source file) "
        f"but found {distinct_ciks}."
    )


def test_each_cik_has_at_least_one_val():
    """Every CIK in the silver model must have at least one row with a non-null val.

    CIKs that never have a val are written to
    dataplatform/test_outputs/sec_company_facts_missing_val.csv for inspection.
    """
    ciks_with_val = _df.filter(pl.col("val").is_not_null()).select("cik").unique()
    all_ciks = _df.select("cik").unique()

    missing_val = all_ciks.join(ciks_with_val, on="cik", how="anti").sort("cik")

    if missing_val.height > 0:
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        missing_val.write_csv(_TEST_OUTPUTS / "sec_company_facts_missing_val.csv")

        affected = missing_val["cik"].drop_nulls().to_list()
        pytest.fail(
            f"{missing_val.height} CIK(s) have no rows with a non-null val.\n"
            f"Affected CIKs ({len(affected)}): {affected[:20]}"
            f"{'...' if len(affected) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/sec_company_facts_missing_val.csv"
        )
