from etl.config import Config
from etl.ingestion.sec import *
from etl.ingestion.yfinance import YFinanceTickerSource
from etl.logger import get_logger

logger = get_logger(__name__)


def injester_maxx(config: Config):
    # yahoo finance
    YFinanceTickerSource(
        "1d",
        dataplatform_root=config.ingestion_dir,
        incremental=config.incremental,
    ).ingest()
    YFinanceTickerSource(
        "1h",
        dataplatform_root=config.ingestion_dir,
        incremental=config.incremental,
    ).ingest()

    # SEC filings
    logger.info("Downloading SEC company tickers...")
    download_sec_tickers(config)
    logger.info("Downloading SEC company facts...")
    download_company_facts(config)
