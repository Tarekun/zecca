from datetime import datetime, timedelta
from pandas import DataFrame, read_parquet, MultiIndex
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import re
import yfinance as yf


def tickers_full_refresh(ticker_names: str | list[str], base_dir: str = "./data_cache"):
    if not isinstance(ticker_names, list):
        ticker_names = [ticker_names]

    df = yf.download(ticker_names, interval="1d", start="1970-01-01")
    if df is not None:
        df = flatten_yf(df)
        save_df(df, "ticker_daily", base_dir)

    df = yf.download(ticker_names, interval="1h", period="2y")
    if df is not None:
        df = flatten_yf(df)
        save_df(df, "ticker_hourly", base_dir)


def tickers_incremental(ticker_names: str | list[str], base_dir: str = "./data_cache"):
    def pull_interval(interval: str):
        table_name = "ticker_daily" if interval == "1h" else "ticker_hourly"
        start = get_latest_partition_date(base_dir, table_name) + timedelta(days=1)
        df = None

        if start is not None:
            print(f"Pulling tickers from date {start}")
            start = start.strftime("%Y-%m-%d")
            df = yf.download(ticker_names, interval=interval, start=start)
        else:
            print("No existing daily data, you should run a full refresh")

        if df is not None and not df.empty:
            df = flatten_yf(df)
            save_df(df, table_name, base_dir)

    pull_interval("1d")
    pull_interval("1h")


def save_df(df: DataFrame, name: str, base_dir: str):
    """Save a DataFrame as a Parquet file. Expects `df` to contain a 'date' column"""
    root = Path(f"{base_dir}/{name}")
    root.mkdir(exist_ok=True)
    df = df.copy()

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    # --- write each partition separately ---
    for year, subset in df.groupby("year"):
        table = pa.Table.from_pandas(subset)
        pq.write_to_dataset(
            table,
            root_path=root,
            partition_cols=["year", "month", "day"],
            compression="snappy",
        )


def flatten_yf(df: DataFrame) -> DataFrame:
    """
    Flatten a yfinance DataFrame with multi-level columns into a long-form table.
    Columns: date, ticker, open, high, low, close, volume
    """
    # handle either multi-indexed or single-ticker df
    if isinstance(df.columns, MultiIndex):
        df = df.stack(level=-1).rename_axis(["date", "ticker"]).reset_index()
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
