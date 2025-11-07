from datetime import datetime, timedelta, timezone
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import re
import yfinance as yf


def tickers_full_refresh(ticker_names: list[str], base_dir: str = "./data_cache"):
    df = yf.download(ticker_names, interval="1d", start="1970-01-01")
    if df is not None:
        df = flatten_yf(df)
        save_df(df, "ticker_daily", base_dir, ["date", "ticker"])

    df = yf.download(ticker_names, interval="1h", period="2y")
    if df is not None:
        df = flatten_yf(df)
        save_df(df, "ticker_hourly", base_dir, ["date", "ticker"])


def tickers_incremental(ticker_names: str | list[str], base_dir: str = "./data_cache"):
    # i be DRY af frfr
    def pull_interval(interval: str):
        table_name = "ticker_hourly" if interval == "1h" else "ticker_daily"
        start = get_latest_partition_date(base_dir, table_name) + timedelta(days=1)
        df = None

        if start is None:
            print(f"No existing {table_name} data, run a full refresh first.")
            return
        if start.date() >= datetime.now(timezone.utc).date():
            print(
                f"{table_name}: data is already up to date (won't pull {start.date()})"
            )
            return

        print(f"Pulling tickers from date {start}")
        start = start.strftime("%Y-%m-%d")
        df = yf.download(ticker_names, interval=interval, start=start)
        if df is not None and not df.empty:
            df = flatten_yf(df)
            save_df(df, table_name, base_dir, ["date", "ticker"])

    pull_interval("1d")
    pull_interval("1h")


def save_df(
    df: DataFrame, name: str, base_dir: str, key_columns: list[str] | None = None
):
    """Save a DataFrame as a Parquet file. Expects `df` to contain a 'date' column.
    If `key_columns` is passed insertion is performed with merging strategy,
    performing deduplication over the specified columns"""

    root = Path(f"{base_dir}/{name}")
    root.mkdir(exist_ok=True)
    df = df.copy()

    df["year"] = df["date"].dt.year
    # df["month"] = df["date"].dt.month
    # df["day"] = df["date"].dt.day

    # --- write each partition separately ---
    # TODO reduce partitioning tbh, this partition will only have 24 rows per ticker so what ~24k?
    for (y,), part_df in df.groupby(["year"]):
        part_path = root / f"year={y}"
        part_path.mkdir(parents=True, exist_ok=True)

        existing_files = list(part_path.glob("*.parquet"))
        # read and deduplicate existing data
        if key_columns is not None and existing_files:
            existing_tables = [pd.read_parquet(f) for f in existing_files]
            existing_df = pd.concat(existing_tables, ignore_index=True)
            combined = pd.concat([existing_df, part_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=key_columns, keep="last")
        else:
            combined = part_df

        # overwrite partition cleanly (remove old files)
        for f in existing_files:
            f.unlink()

        # write merged partition
        file_path = part_path / f"part-{name}-{y}.parquet"
        table = pa.Table.from_pandas(combined)
        pq.write_table(table, file_path, compression="snappy")


def flatten_yf(df: DataFrame) -> DataFrame:
    """
    Flatten a yfinance DataFrame with multi-level columns into a long-form table.
    Columns: date, ticker, open, high, low, close, volume
    """
    # handle either multi-indexed or single-ticker df
    if isinstance(df.columns, pd.MultiIndex):
        df = (
            df.stack(level=-1, future_stack=True)
            .rename_axis(["date", "ticker"])
            .reset_index()
        )
    else:
        df = df.reset_index()
        df["ticker"] = "UNKNOWN"  # or pass it in manually if single ticker
        df = df.rename(columns=str.lower)

    df.columns = [c.lower() for c in df.columns]
    return df


def get_latest_partition_date(base_dir: str, name: str) -> datetime:
    """Find the latest year/month/day= partition in a dataset directory"""
    path = Path(f"{base_dir}/{name}")

    pattern = re.compile(r"year=(\d{4}).*month=(\d{1,2}).*day=(\d{1,2})")
    latest = None
    for p in path.rglob("day=*"):
        m = pattern.search(str(p))
        if m:
            y, mo, d = map(int, m.groups())
            dt = datetime(y, mo, d)
            if latest is None or dt > latest:
                latest = dt

    if latest is None:
        raise Exception("i dont know what to say")
    return latest


tickers_full_refresh(["msft"])
