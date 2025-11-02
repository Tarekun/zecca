from datetime import datetime
from pandas import DataFrame, read_parquet
import yfinance as yf
from pathlib import Path

FOLDER = "data_cache"
Path(FOLDER).mkdir(exist_ok=True)


def backup_ticker(ticker_names: str | list[str]):
    # start = datetime(2000, 01,)
    if not isinstance(ticker_names, list):
        ticker_names = [ticker_names]

    for ticker in ticker_names:
        coarse = yf.download(ticker_names, period="10y", interval="1d")
        if coarse is not None:
            save_df(coarse, f"{ticker}_1d")
        # TODO support hourly data ingestion soon
        # dense = yf.download(ticker_names, period="10y", interval="1h")
        # if dense is not None:
        #     save_df(dense, f"{ticker}_1h")


def save_df(df: DataFrame, name: str):
    """Save a DataFrame as a Parquet file"""

    path = Path(FOLDER) / f"{name}.parquet"
    df.to_parquet(path, index=True, engine="pyarrow")
    print(f"Saved to {path}")


def load_df(name: str) -> DataFrame:
    """Load a DataFrame from a Parquet file."""
    path = Path(FOLDER) / f"{name}.parquet"
    return read_parquet(path, engine="pyarrow")
