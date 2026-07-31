import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).parents[2]))
from etl.ingestion.yfinance import upsert_df


def test_upsert_df_inserts_when_no_existing_data(tmp_path):
    df = pl.DataFrame({"date": [1, 2], "ticker": ["AAPL", "AAPL"], "close": [10.0, 11.0]})

    upsert_df(df, "prices", tmp_path, key_columns=["date", "ticker"])

    result = pl.read_parquet(tmp_path / "prices" / "*.parquet")
    assert result.sort("date")["close"].to_list() == [10.0, 11.0]


def test_upsert_df_updates_matching_keys_and_keeps_others(tmp_path):
    first = pl.DataFrame({"date": [1, 2, 3], "ticker": ["AAPL"] * 3, "close": [10.0, 11.0, 12.0]})
    upsert_df(first, "prices", tmp_path, key_columns=["date", "ticker"])

    # re-pulls date=2 with a revised value and introduces a brand new date=4
    second = pl.DataFrame({"date": [2, 4], "ticker": ["AAPL", "AAPL"], "close": [99.0, 13.0]})
    upsert_df(second, "prices", tmp_path, key_columns=["date", "ticker"])

    result = pl.read_parquet(tmp_path / "prices" / "*.parquet").sort("date")
    assert result["date"].to_list() == [1, 2, 3, 4]
    assert result["close"].to_list() == [10.0, 99.0, 12.0, 13.0]


def test_upsert_df_never_leaves_duplicate_keys_on_disk(tmp_path):
    for value in [1.0, 2.0, 3.0]:
        df = pl.DataFrame({"date": [1], "ticker": ["AAPL"], "close": [value]})
        upsert_df(df, "prices", tmp_path, key_columns=["date", "ticker"])

    result = pl.read_parquet(tmp_path / "prices" / "*.parquet")
    assert result.height == 1
    assert result["close"].to_list() == [3.0]


def test_upsert_df_last_row_wins_on_duplicate_keys_within_same_batch(tmp_path):
    # simulates yfinance returning the same (date, ticker) twice in one pull;
    # the last occurrence must be the one kept, mirroring "last read wins"
    df = pl.DataFrame({"date": [1, 1], "ticker": ["AAPL", "AAPL"], "close": [10.0, 20.0]})

    upsert_df(df, "prices", tmp_path, key_columns=["date", "ticker"])

    result = pl.read_parquet(tmp_path / "prices" / "*.parquet")
    assert result.height == 1
    assert result["close"].to_list() == [20.0]


def test_upsert_df_partitions_hive_style_and_scopes_merge_per_partition(tmp_path):
    first = pl.DataFrame(
        {
            "date": [1, 2],
            "ticker": ["AAPL", "AAPL"],
            "close": [10.0, 20.0],
            "year": [2023, 2024],
            "month": [1, 1],
        }
    )
    upsert_df(first, "prices", tmp_path, key_columns=["date", "ticker"], partition_columns=["year", "month"])

    assert (tmp_path / "prices" / "year=2023" / "month=1").is_dir()
    assert (tmp_path / "prices" / "year=2024" / "month=1").is_dir()

    # only touches the 2024/1 partition; 2023/1 must be left untouched
    second = pl.DataFrame(
        {"date": [2], "ticker": ["AAPL"], "close": [99.0], "year": [2024], "month": [1]}
    )
    upsert_df(second, "prices", tmp_path, key_columns=["date", "ticker"], partition_columns=["year", "month"])

    untouched = pl.read_parquet(tmp_path / "prices" / "year=2023" / "month=1" / "*.parquet")
    assert untouched["close"].to_list() == [10.0]

    updated = pl.read_parquet(tmp_path / "prices" / "year=2024" / "month=1" / "*.parquet")
    assert updated["close"].to_list() == [99.0]

    full = pl.scan_parquet(str(tmp_path / "prices" / "**" / "*.parquet"), hive_partitioning=True).collect()
    assert full.sort("date")["close"].to_list() == [10.0, 99.0]


def test_upsert_df_partition_columns_must_already_be_on_the_dataframe(tmp_path):
    # partition_columns is purely a read of existing df columns, not a
    # derivation from "date" or anything else -- missing columns must fail
    df = pl.DataFrame({"date": [1], "ticker": ["AAPL"], "close": [10.0]})

    try:
        upsert_df(df, "prices", tmp_path, key_columns=["date", "ticker"], partition_columns=["year", "month"])
        assert False, "expected a failure when partition columns are missing from df"
    except pl.exceptions.ColumnNotFoundError:
        pass
