import duckdb
import pandas as pd
from analysis.db.globals import YFINANCE_DIR


def select_ticker(
    interval: str,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    ticker: str | list[str] | None = None,
) -> pd.DataFrame:
    """Ticker selection function `interval` is either '1d' or '1h'.
    Returns a pandas DataFrame"""

    table = "ticker_daily" if interval == "1d" else "ticker_hourly"
    parquet_path = f"{YFINANCE_DIR}/{table}"
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_path}/**/*.parquet', hive_partitioning=true)
        WHERE TRUE
    """

    if year is not None:
        query += _values_query("year", year)
    if month is not None:
        query += _values_query("month", month)
    if ticker is not None:
        if not isinstance(ticker, list):
            ticker = [ticker]
        query += _values_query("ticker", [f"'{t.upper()}'" for t in ticker])

    return run_custom_query(query)


def read_tickers(base_dir: str) -> pd.DataFrame:
    file_path = f"{base_dir}/company_tickers.parquet"
    return run_custom_query(
        f"""
        SELECT *
        FROM read_parquet('{file_path}')
    """
    )


def run_custom_query(query: str):
    con = duckdb.connect(database=":memory:")
    df = con.execute(query).fetch_df()
    con.close()
    return df


def _values_query(col: str, values) -> str:
    if isinstance(values, list):
        values = ", ".join(str(val) for val in values)
        return f" AND {col} IN ({values})"
    else:
        return f" AND {col} = {values}"
