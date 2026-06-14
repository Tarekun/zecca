import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

_PROJECT_ROOT = Path(__file__).parents[3]
_SILVER_PARQUET = (
    _PROJECT_ROOT / "dataplatform" / "silver" / "sec_company_facts" / "sec_company_facts.parquet"
)
_RAW_SEC_DIR = _PROJECT_ROOT / "dataplatform" / "raw" / "sec"
_TEST_OUTPUTS = _PROJECT_ROOT / "dataplatform" / "test_outputs"


def test_cik_count_matches_file_count():
    """The number of distinct CIK values in the silver model must equal the number
    of source JSON files under dataplatform/raw/sec — one row (possibly null) per file."""
    df = pl.read_parquet(_SILVER_PARQUET)
    file_count = len(list(_RAW_SEC_DIR.glob("*.json")))
    distinct_ciks = df["cik"].n_unique()

    assert distinct_ciks == file_count, (
        f"Expected {file_count} distinct CIK values (one per source file) "
        f"but found {distinct_ciks}."
    )


def test_each_cik_has_at_least_one_val():
    """Every CIK in the silver model must have at least one row with a non-null val.

    CIKs that never have a val are written to
    dataplatform/test_outputs/sec_company_facts_missing_val.csv for inspection.
    """
    df = pl.read_parquet(_SILVER_PARQUET)

    ciks_with_val = df.filter(pl.col("val").is_not_null()).select("cik").unique()
    all_ciks = df.select("cik").unique()

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
