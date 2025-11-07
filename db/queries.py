import duckdb
import pandas as pd
from datetime import datetime
from typing import Optional


def select_ticker(
    interval: str,
    base_dir: str = "./data_cache",
    year: int | list[int] | None = None,
    ticker: str | list[str] | None = None,
) -> pd.DataFrame:
    """Ticker selection function `interval` is either '1d' or '1h'.
    Returns a pandas DataFrame"""

    table = "ticker_daily" if interval == "1d" else "ticker_hourly"
    parquet_path = f"{base_dir}/{table}"
    con = duckdb.connect(database=":memory:")
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_path}/**/*.parquet', hive_partitioning=true)
        WHERE 1=1
    """

    if year is not None:
        if isinstance(year, list):
            years_str = ", ".join(str(y) for y in year)
            query += f" AND year IN ({years_str})"
        else:
            query += f" AND year = {int(year)}"
    if ticker is not None:
        if isinstance(ticker, list):
            tickers_str = ", ".join(f"'{t.upper()}'" for t in ticker)
            query += f" AND ticker IN ({tickers_str})"
        else:
            query += f" AND ticker = '{ticker.upper()}'"

    df = con.execute(query).fetch_df()
    con.close()
    return df
