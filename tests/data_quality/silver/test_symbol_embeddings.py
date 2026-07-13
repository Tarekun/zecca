import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.silver.symbol_embeddings import SymbolEmbeddingsSilver

_TEST_OUTPUTS = Path(__file__).parents[3] / "dataplatform" / "test_outputs"

_df = SymbolEmbeddingsSilver().read_from_disk().collect()
_EMBEDDING_COLS = [c for c in _df.columns if c.startswith("e")]

# Large-cap symbols spanning distinct sectors (tech, financial, energy) that are
# expected to be present in every not_before partition of the dataset.
_WELL_KNOWN_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "JPM", "BAC", "XOM", "CVX", "V"]
_MIN_ADJACENT_SIMILARITY = 0.75


def test_no_null_symbol():
    """No row in symbol_embeddings should have a null symbol."""
    null_rows = _df.filter(pl.col("symbol").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null symbol.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_no_null_not_before():
    """No row in symbol_embeddings should have a null not_before."""
    null_rows = _df.filter(pl.col("not_before").is_null())

    assert null_rows.height == 0, (
        f"Found {null_rows.height} row(s) with a null not_before.\n"
        f"Sample (up to 20):\n{null_rows.head(20)}"
    )


def test_not_before_symbol_is_a_key():
    """(not_before, symbol) must uniquely identify a row."""
    duplicates = (
        _df.group_by(["not_before", "symbol"])
        .agg(pl.len().alias("occurrences"))
        .filter(pl.col("occurrences") > 1)
    )

    assert duplicates.height == 0, (
        f"Found {duplicates.height} duplicate (not_before, symbol) pair(s).\n"
        f"Sample (up to 20):\n{duplicates.head(20)}"
    )


def test_embedding_columns_are_finite():
    """Every embedding component must be a finite, non-null number."""
    is_bad = pl.any_horizontal(
        [pl.col(c).is_null() | ~pl.col(c).is_finite() for c in _EMBEDDING_COLS]
    )
    bad_rows = _df.filter(is_bad)

    assert bad_rows.height == 0, (
        f"Found {bad_rows.height} row(s) with a null/non-finite embedding component.\n"
        f"Sample (up to 20):\n{bad_rows.head(20)}"
    )


def test_well_known_symbols_present_in_every_partition():
    """Sanity check on the fixture set itself: each well-known symbol must be
    present in every not_before partition, otherwise the adjacency test below
    would silently skip periods instead of comparing them."""
    n_partitions = _df.select("not_before").unique().height
    counts = (
        _df.filter(pl.col("symbol").is_in(_WELL_KNOWN_SYMBOLS))
        .group_by("symbol")
        .agg(pl.len().alias("n_partitions"))
    )

    missing = counts.filter(pl.col("n_partitions") < n_partitions)
    found_symbols = counts["symbol"].to_list()
    absent_entirely = [s for s in _WELL_KNOWN_SYMBOLS if s not in found_symbols]

    assert missing.height == 0 and not absent_entirely, (
        f"Expected all of {_WELL_KNOWN_SYMBOLS} in all {n_partitions} partitions.\n"
        f"Entirely absent: {absent_entirely}\n"
        f"Present in fewer than all partitions:\n{missing}"
    )


def test_well_known_symbols_stable_across_adjacent_partitions():
    """For a set of well-known, large-cap symbols, the (Procrustes-aligned)
    embedding should not change direction abruptly between adjacent rolling
    windows: cosine similarity between consecutive not_before partitions must
    stay above 0.75.

    Violations are written to
    dataplatform/test_outputs/symbol_embeddings_low_adjacent_similarity.csv
    for inspection.
    """
    dates = sorted(_df.select("not_before").unique().to_series().to_list())
    violations = []

    for symbol in _WELL_KNOWN_SYMBOLS:
        sub = (
            _df.filter(pl.col("symbol") == symbol)
            .sort("not_before")
            .select(["not_before", *_EMBEDDING_COLS])
        )
        by_date = {row[0]: np.array(row[1:], dtype=float) for row in sub.iter_rows()}

        for prev_date, next_date in zip(dates, dates[1:]):
            if prev_date not in by_date or next_date not in by_date:
                continue  # covered by test_well_known_symbols_present_in_every_partition

            a, b = by_date[prev_date], by_date[next_date]
            cos_sim = a.dot(b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12)

            if cos_sim < _MIN_ADJACENT_SIMILARITY:
                violations.append(
                    {
                        "symbol": symbol,
                        "not_before": prev_date,
                        "next_not_before": next_date,
                        "cosine_similarity": cos_sim,
                    }
                )

    if violations:
        _TEST_OUTPUTS.mkdir(parents=True, exist_ok=True)
        violations_df = pl.DataFrame(violations)
        violations_df.write_csv(
            _TEST_OUTPUTS / "symbol_embeddings_low_adjacent_similarity.csv"
        )

        pytest.fail(
            f"Found {len(violations)} adjacent-partition pair(s) with cosine "
            f"similarity below {_MIN_ADJACENT_SIMILARITY} for well-known symbols.\n"
            f"Full list written to dataplatform/test_outputs/symbol_embeddings_low_adjacent_similarity.csv\n"
            f"Sample (up to 20):\n{violations_df.head(20)}"
        )


def test_embedding_dimension_matches_schema():
    """Every partition should expose the same number of embedding columns as
    the model's declared embedding_size."""
    expected = SymbolEmbeddingsSilver().embedding_size

    assert len(_EMBEDDING_COLS) == expected, (
        f"Expected {expected} embedding columns (e0..e{expected - 1}), "
        f"found {len(_EMBEDDING_COLS)}: {_EMBEDDING_COLS}"
    )
