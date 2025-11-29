from datetime import date, datetime, timedelta, timezone
import math
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import re
from time import sleep
import yfinance as yf
from analysis.db.queries import read_tickers, run_custom_query


def yfincance_ticker_ingestion(interval: str, base_dir: str, incremental: bool = True):
    tickers = read_tickers(base_dir)
    ticker_names = tickers["ticker"].dropna().astype(str).unique().tolist()
    total = len(ticker_names)
    batch_size = 100 if incremental else 50
    num_batches = math.ceil(total / batch_size)
    table_name = "ticker_daily" if interval == "1d" else "ticker_hourly"

    # request parameters defaults
    start = "1970-01-01"
    period = None
    if incremental:
        # tbh i think reprocessing the last day every day is best
        start_date: datetime = _get_latest_partition_date(base_dir, table_name)
        # start_date = get_latest_partition_date(base_dir, table_name) + timedelta(days=1)
        if start_date is None:
            print(f"No existing {table_name} data, run a full refresh first.")
            return
        # TODO review timezone handling
        if start_date.replace(tzinfo=timezone.utc) >= datetime.now(timezone.utc):
            print(f"{table_name}: data is already up to date (won't pull {start_date})")
            return
        start = start_date.strftime("%Y-%m-%d")
        print(f"Incremental ingestion start date: {start}")
    if not incremental and interval == "1h":
        period = "2y"
        start = None

    full_data = pd.DataFrame()
    for i in range(0, total, batch_size):
        try:
            batch = ticker_names[i : i + batch_size]
            print(
                f"Processing batch {i // batch_size + 1}/{num_batches}: {len(batch)} tickers"
            )

            df = yf.download(batch, interval=interval, period=period, start=start)
            if df is not None and not df.empty:
                df = _flatten_yf(df)
                full_data = pd.concat([full_data, df], ignore_index=True)
            sleep(30)
        except Exception as e:
            print(f"⚠️  Batch {i // batch_size + 1} failed: {e}")

    _save_df(full_data, table_name, base_dir, ["date", "ticker"])


def _flatten_yf(df: DataFrame) -> DataFrame:
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


def _save_df(
    df: DataFrame, name: str, base_dir: str, key_columns: list[str] | None = None
):
    """Save a DataFrame as a Parquet file. Expects `df` to contain a 'date' column.
    If `key_columns` is passed insertion is performed with merging strategy,
    performing deduplication over the specified columns"""

    Path(base_dir).mkdir(exist_ok=True)
    root = Path(f"{base_dir}/{name}")
    root.mkdir(exist_ok=True)
    df = df.copy()

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    # df["day"] = df["date"].dt.day

    # --- write each partition separately ---
    for (y, m), part_df in df.groupby(["year", "month"]):
        part_path = root / f"year={y}/month={m}"
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
        file_path = part_path / f"part-{name}-{y}{m}.parquet"
        table = pa.Table.from_pandas(combined)
        pq.write_table(table, file_path, compression="snappy")


def _get_latest_partition_date(base_dir: str, name: str) -> datetime:
    """Find the latest year/month partition in a dataset directory"""

    path = Path(base_dir) / name
    pattern = re.compile(r"year=(\d{4})[/\\]month=(\d{1,2})[/\\]?$")
    latest_year_month = None

    # walk all subdirectories and look for year=YYYY/month=MM pattern
    for p in path.rglob("*"):
        if p.is_dir():
            match = pattern.search(str(p))
            if match:
                y, m = int(match.group(1)), int(match.group(2))
                if latest_year_month is None or (y, m) > latest_year_month:
                    latest_year_month = (y, m)
    if latest_year_month is None:
        raise FileNotFoundError(f"No year/month partitions found under {path}")

    year, month = latest_year_month
    res = run_custom_query(
        f"""SELECT MAX(date)
        FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        WHERE year={year}
        AND month={month}"""
    )
    return res["max(date)"][0].to_pydatetime()
