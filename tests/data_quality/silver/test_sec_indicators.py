import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.sec_indicators import SecIndicatorsSilver

_PROJECT_ROOT = Path(__file__).parents[3]
_RAW_SEC_DIR = _PROJECT_ROOT / "dataplatform" / "raw" / "sec"
_TEST_OUTPUTS = _PROJECT_ROOT / "dataplatform" / "test_outputs"

_df = SecIndicatorsSilver().read_from_disk().collect()


def test_no_null_indicator():
    """No row in sec_indicators should have a null indicator key."""
    null_rows = _df.filter(pl.col("indicator").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null indicator.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_label():
    """Every indicator must have a label.

    Indicators missing a label are written to
    dataplatform/test_outputs/sec_indicators_missing_label.csv for inspection.
    """
    null_rows = _df.filter(pl.col("label").is_null())

    if null_rows.height > 0:
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        null_rows.write_csv(_TEST_OUTPUTS / "sec_indicators_missing_label.csv")

        pytest.fail(
            f"Found {null_rows.height} indicator(s) with a null label.\n"
            f"Full list written to dataplatform/test_outputs/sec_indicators_missing_label.csv\n"
            f"Sample (up to 20):\n{null_rows.head(20)}"
        )


def test_namespace_values_are_valid():
    """All namespace values must be either 'dei' or 'us-gaap'."""
    valid = {"dei", "us-gaap"}
    invalid = _df.filter(~pl.col("namespace").is_in(list(valid)))

    assert invalid.height == 0, (
        f"Found {invalid.height} row(s) with an unexpected namespace value.\n"
        f"Sample (up to 20):\n{invalid.head(20)}"
    )


def test_cik_count_positive():
    """Every row must have a cik_count of at least 1."""
    bad_rows = _df.filter(pl.col("cik_count") < 1)

    assert bad_rows.height == 0, (
        f"Found {bad_rows.height} row(s) with cik_count < 1.\n"
        f"Sample (up to 20):\n{bad_rows.head(20)}"
    )


def test_cik_count_does_not_exceed_source_file_count():
    """No indicator's cik_count can exceed the total number of source SEC JSON files."""
    file_count = len(list(_RAW_SEC_DIR.glob("*.json")))
    over_count = _df.filter(pl.col("cik_count") > file_count)

    assert over_count.height == 0, (
        f"Found {over_count.height} indicator(s) with cik_count > {file_count} (total source files).\n"
        f"Sample (up to 20):\n{over_count.head(20)}"
    )


def test_no_duplicate_tuples():
    """Each (namespace, indicator, label, description) tuple must be unique."""
    key_cols = ["namespace", "indicator", "label", "description"]
    duplicates = (
        _df.group_by(key_cols)
        .agg(pl.len().alias("occurrences"))
        .filter(pl.col("occurrences") > 1)
    )

    assert duplicates.height == 0, (
        f"Found {duplicates.height} duplicate (namespace, indicator, label, description) tuple(s).\n"
        f"Sample (up to 20):\n{duplicates.head(20)}"
    )
