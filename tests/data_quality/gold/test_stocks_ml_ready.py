import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.gold.stocks_ml_ready import StocksMlReadyGold
from etl.transformation.silver.symbol_embeddings import SymbolEmbeddingsSilver
from etl.transformation.silver.good_symbols import GoodSymbolsSilver

_TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

_lf = StocksMlReadyGold().read_from_disk()


def test_no_null_symbol():
    """No row in stocks_ml_ready should have a null symbol."""
    null_rows = _lf.select("symbol").filter(pl.col("symbol").is_null()).collect()

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null symbol.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_timeframe():
    """No row in stocks_ml_ready should have a null timeframe."""
    null_rows = _lf.select("timeframe").filter(pl.col("timeframe").is_null()).collect()

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null timeframe.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_symbol_timeframe_is_a_key():
    """(symbol, timeframe) must uniquely identify a row: the good_symbols and
    symbol_embeddings joins must not fan out any row."""
    duplicates = (
        _lf.select(["symbol", "timeframe"])
        .group_by(["symbol", "timeframe"])
        .agg(pl.len().alias("occurrences"))
        .filter(pl.col("occurrences") > 1)
        .collect()
    )

    assert duplicates.height == 0, (
        f"Found {duplicates.height} duplicate (symbol, timeframe) pair(s).\n"
        f"Sample (up to 20):\n{duplicates.head(20)}"
    )


def test_no_null_embedding():
    """No row should be missing its `embedding` (the symbol_embeddings as-of join).

    Violations are written to
    dataplatform/test_outputs/ml_ready_missing_embeddings.csv for inspection.
    """
    null_rows = (
        _lf.filter(pl.col("embedding").is_null())
        .select(["symbol", "timeframe"])
        .collect()
    )

    if null_rows.height > 0:
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        null_rows.sort(["symbol", "timeframe"]).write_csv(
            _TEST_OUTPUTS / "ml_ready_missing_embeddings.csv"
        )

        affected_symbols = null_rows["symbol"].unique().sort().to_list()
        pytest.fail(
            f"{null_rows.height} row(s) are missing an embedding.\n"
            f"Affected symbols ({len(affected_symbols)}): {affected_symbols[:20]}"
            f"{'...' if len(affected_symbols) > 20 else ''}\n"
            f"Full list written to dataplatform/test_outputs/ml_ready_missing_embeddings.csv"
        )


def test_embedding_dimension_matches_schema():
    """Every non-null embedding must have exactly `embedding_size` components."""
    expected = SymbolEmbeddingsSilver().embedding_size

    lengths = (
        _lf.filter(pl.col("embedding").is_not_null())
        .select(pl.col("embedding").list.len().alias("length"))
        .unique()
        .collect()
    )

    assert lengths["length"].to_list() == [expected], (
        f"Expected every embedding to have {expected} components, "
        f"found length(s): {lengths['length'].to_list()}"
    )


def test_symbols_are_restricted_to_good_symbols():
    """Every (symbol, timeframe) pair must be present in silver.good_symbols:
    the inner join must not let any other symbol through."""
    good_symbols = GoodSymbolsSilver().read_from_disk()

    orphans = (
        _lf.select(["symbol", "timeframe"])
        .join(good_symbols, on=["symbol", "timeframe"], how="anti")
        .collect()
    )

    assert orphans.height == 0, (
        f"Found {orphans.height} row(s) whose (symbol, timeframe) is not in "
        f"silver.good_symbols.\nSample (up to 20):\n{orphans.head(20)}"
    )
