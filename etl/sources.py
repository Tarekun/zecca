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
from etl.utils import yfincance_ticker_ingestion


def ingest_ticker_daily(base_dir: str, incremental: bool = True):
    yfincance_ticker_ingestion("1d", base_dir, incremental)


def ingest_ticker_hourly(base_dir: str, incremental: bool = True):
    yfincance_ticker_ingestion("1h", base_dir, incremental)
