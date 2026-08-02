from pathlib import Path

from etl.config import Config
from etl.ingestion.sec import SecCompanyFacts, SecTickers
from etl.ingestion.yfinance import YFinanceTicker


def injester_maxx(config: Config):
    # config.ingestion_dir is the "raw" layer directory itself (e.g.
    # "dataplatform/raw"); sources want the platform root and append their
    # own "raw" layer, hence the parent here.
    dataplatform_root = str(Path(config.ingestion_dir).parent)

    # SEC filings
    SecTickers(dataplatform_root=dataplatform_root).ingest()
    SecCompanyFacts(dataplatform_root=dataplatform_root).ingest()

    # yahoo finance
    YFinanceTicker(
        "1d",
        dataplatform_root=dataplatform_root,
        incremental=config.incremental,
    ).ingest()
    YFinanceTicker(
        "1h",
        dataplatform_root=dataplatform_root,
        incremental=config.incremental,
    ).ingest()
