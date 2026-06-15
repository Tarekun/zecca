import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

_PROJECT_ROOT = Path(__file__).parents[3]
_SILVER_PARQUET = (
    _PROJECT_ROOT / "dataplatform" / "silver" / "company_tickers" / "company_tickers.parquet"
)


def test_no_null_cik_str():
    """No row in company_tickers should have a null cik_str."""
    df = pl.read_parquet(_SILVER_PARQUET)
    null_rows = df.filter(pl.col("cik_str").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null cik_str.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_ticker():
    """No row in company_tickers should have a null ticker."""
    df = pl.read_parquet(_SILVER_PARQUET)
    null_rows = df.filter(pl.col("ticker").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null ticker.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )
