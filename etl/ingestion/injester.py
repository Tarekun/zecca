from etl.config import Config
from etl.ingestion.sec import *
from etl.ingestion.yfinance import *


def injester_maxx(config: Config):
    # yahoo finance
    logger.info("Starting ticker daily ingestion...")
    ingest_ticker_daily(base_dir=config.ingestion_dir, incremental=config.incremental)
    logger.info("Starting ticker hourly ingestion...")
    ingest_ticker_hourly(base_dir=config.ingestion_dir, incremental=config.incremental)

    # SEC filings
    logger.info("Downloading SEC company tickers...")
    download_sec_tickers(config)
    logger.info("Downloading SEC company facts...")
    download_company_facts(config)
