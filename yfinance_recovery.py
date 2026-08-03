"""One-off recovery tool: re-pull a symbol's data from Yahoo Finance, diff it
against what's stored on disk, and (with confirmation) upsert the corrected
rows.

Usage:
    python -m etl.ingestion.yfinance_recovery --symbol AAPL \\
        --start 2024-01-01 --end 2024-02-01 --interval 1d
"""

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl
import yfinance as yf

from analysis.db.globals import YFINANCE_DIR
from analysis.db.queries import select_ticker
from etl.ingestion.yfinance import _flatten_yf
from etl.logger import get_logger
from etl.utils import upsert_df

logger = get_logger(__name__)

COMPARE_COLUMNS = ["open", "high", "low", "close", "volume"]
DEFAULT_TOLERANCE = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-pull a symbol from Yahoo Finance, diff against stored "
        "data, and optionally upsert the corrected rows."
    )
    parser.add_argument("--symbol", required=True, help="Ticker symbol, e.g. AAPL")
    parser.add_argument("--start", required=True, help="Start date, YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="End date, YYYY-MM-DD (exclusive)")
    parser.add_argument("--interval", choices=["1d", "1h"], default="1d")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help="Relative difference (e.g. 0.01 = 1%%) above which a value counts "
        "as a mismatch; missing rows always count regardless (default: 0, i.e. "
        "any difference counts)",
    )
    return parser.parse_args()


def pull_fresh_data(symbol: str, start: str, end: str, interval: str) -> pl.DataFrame:
    logger.info("Pulling %s (%s) from yfinance: %s -> %s", symbol, interval, start, end)
    df = yf.download([symbol], interval=interval, start=start, end=end)
    if df is None or df.empty:
        raise ValueError(f"yfinance returned no data for {symbol} in [{start}, {end})")

    df = _flatten_yf(df)
    fresh = pl.from_pandas(df)
    return fresh.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
    )


def load_stored_data(symbol: str, start: str, end: str, interval: str) -> pl.DataFrame:
    table_name = "ticker_daily" if interval == "1d" else "ticker_hourly"
    if not (Path(YFINANCE_DIR) / table_name).exists():
        logger.warning("No stored %s table found, treating existing data as empty.", table_name)
        return pl.DataFrame()

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    years = list(range(start_dt.year, end_dt.year + 1))

    stored = select_ticker(interval, year=years, ticker=symbol)
    stored = pl.from_pandas(stored)
    if stored.is_empty():
        return stored
    return stored.filter((pl.col("date") >= start_dt) & (pl.col("date") < end_dt))


def diff_data(fresh: pl.DataFrame, stored: pl.DataFrame, tolerance: float = DEFAULT_TOLERANCE) -> pl.DataFrame:
    fresh_k = fresh.select(["date", "ticker", *COMPARE_COLUMNS])

    if stored.is_empty():
        stored_k = fresh_k.clear().rename({c: f"{c}_stored" for c in COMPARE_COLUMNS})
    else:
        stored_k = stored.select(["date", "ticker", *COMPARE_COLUMNS]).rename(
            {c: f"{c}_stored" for c in COMPARE_COLUMNS}
        )

    joined = fresh_k.join(stored_k, on=["date", "ticker"], how="full", coalesce=True).sort("date")

    diff_exprs = [
        (
            pl.col(f"{c}_stored").is_null()
            | pl.col(c).is_null()
            | (
                (pl.col(c) - pl.col(f"{c}_stored")).abs()
                > tolerance * pl.max_horizontal(pl.col(f"{c}_stored").abs(), pl.col(c).abs())
            )
        ).alias(f"{c}_diff")
        for c in COMPARE_COLUMNS
    ]
    joined = joined.with_columns(diff_exprs)
    any_diff = pl.any_horizontal([pl.col(f"{c}_diff") for c in COMPARE_COLUMNS])
    return joined.filter(any_diff)


def align_schema(fresh: pl.DataFrame, base_dir: str, table_name: str) -> pl.DataFrame:
    """Align `fresh`'s columns/dtypes with whatever is already on disk in each
    partition it will land in (e.g. a legacy "adj close" column, a stray
    pandas "__index_level_0__" column, or int/float drift on volume), so the
    merge in `upsert_df` doesn't choke on mismatched schemas. Schemas can
    differ partition to partition, so each one is checked against its own
    existing file rather than some arbitrary file elsewhere in the table."""
    table_path = Path(base_dir) / table_name
    aligned_parts = []
    for (year, month), part_df in fresh.group_by(["year", "month"], maintain_order=True):
        existing_files = list((table_path / f"year={year}" / f"month={month}").glob("*.parquet"))
        if existing_files:
            existing_schema = pl.read_parquet_schema(existing_files[0])
            for col, dtype in existing_schema.items():
                if col not in part_df.columns:
                    part_df = part_df.with_columns(pl.lit(None, dtype=dtype).alias(col))
                elif part_df.schema[col] != dtype:
                    part_df = part_df.with_columns(pl.col(col).cast(dtype))
            # "vertical_relaxed" concat in upsert_df is order-sensitive, not
            # just name/dtype-sensitive -- match the on-disk column order.
            extra_cols = [c for c in part_df.columns if c not in existing_schema]
            part_df = part_df.select([*existing_schema.keys(), *extra_cols])
        aligned_parts.append(part_df)
    return pl.concat(aligned_parts, how="diagonal_relaxed")


def print_diffs(diffs: pl.DataFrame) -> None:
    print(f"\nFound {diffs.height} differing row(s):\n")
    for row in diffs.iter_rows(named=True):
        print(f"  {row['date']}  {row['ticker']}")
        for c in COMPARE_COLUMNS:
            if row[f"{c}_diff"]:
                stored_val = row[f"{c}_stored"]
                fresh_val = row[c]
                if stored_val is None and fresh_val is None:
                    print(f"      {c}: hole in storage, but yfinance has no data for this date either (non-trading day?)")
                elif stored_val is None:
                    print(f"      {c}: MISSING IN STORAGE -> {fresh_val}")
                elif fresh_val is None:
                    print(f"      {c}: {stored_val} -> MISSING FROM YFINANCE PULL")
                else:
                    print(f"      {c}: {stored_val} -> {fresh_val}")
    print()


def main() -> None:
    args = parse_args()

    fresh = pull_fresh_data(args.symbol, args.start, args.end, args.interval)
    stored = load_stored_data(args.symbol, args.start, args.end, args.interval)

    diffs = diff_data(fresh, stored, args.tolerance)
    if diffs.is_empty():
        print("No differences found, nothing to do.")
        return

    print_diffs(diffs)

    answer = input("Upsert the re-pulled data to fix these rows? [y/N] ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborted, no changes written.")
        return

    table_name = "ticker_daily" if args.interval == "1d" else "ticker_hourly"
    fresh = align_schema(fresh, YFINANCE_DIR, table_name)
    upsert_df(fresh, table_name, YFINANCE_DIR, ["date", "ticker"], ["year", "month"])
    print(f"Upserted {fresh.height} row(s) into {YFINANCE_DIR}/{table_name}.")


if __name__ == "__main__":
    main()
