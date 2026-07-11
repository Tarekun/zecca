import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.company_tickers import CompanyTickersSilver

_TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

_df = CompanyTickersSilver().read_from_disk().collect()


def test_no_null_cik_str():
    """No row in company_tickers should have a null cik_str."""
    null_rows = _df.filter(pl.col("cik_str").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null cik_str.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_ticker():
    """No row in company_tickers should have a null ticker."""
    null_rows = _df.filter(pl.col("ticker").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null ticker.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )
