from pathlib import Path

from etl.config import Config
from etl.ingestion.french_library import FrenchLibrary
from etl.ingestion.sec import (
    SecCompanyFacts,
    SecFilingTextTickers,
    SecFullIndex,
    SecTickers,
    SecTickersExchange,
    SecXbrlCoverPages,
    SecXbrlFilings,
)
from etl.ingestion.source import Source
from etl.ingestion.yfinance import YFinanceTicker


def configured_sources(config: Config) -> list[Source]:
    # config.ingestion_dir is the "raw" layer directory itself (e.g.
    # "dataplatform/raw"); sources want the platform root and append their
    # own "raw" layer, hence the parent here.
    dataplatform_root = str(Path(config.ingestion_dir).parent)

    sources: list[Source] = [
        # SEC filings
        SecTickers(dataplatform_root=dataplatform_root),
        SecTickersExchange(dataplatform_root=dataplatform_root),
        SecCompanyFacts(dataplatform_root=dataplatform_root),
        # EDGAR bulk history for the security master (always incremental:
        # past quarters/datasets are immutable and never downloaded twice)
        SecFullIndex(dataplatform_root=dataplatform_root),
        SecXbrlCoverPages(dataplatform_root=dataplatform_root),
        SecXbrlFilings(dataplatform_root=dataplatform_root),
        # after the two above: reads the annual reports of the companies
        # they give no ticker for (one download per company, slow the
        # first time)
        SecFilingTextTickers(dataplatform_root=dataplatform_root),
        # yahoo finance
        YFinanceTicker(
            "1d",
            dataplatform_root=dataplatform_root,
            incremental=config.incremental,
        ),
        YFinanceTicker(
            "1h",
            dataplatform_root=dataplatform_root,
            incremental=config.incremental,
        ),
        # Fama-French factors
        # *[
        #     FrenchLibrary(region, dataplatform_root=dataplatform_root)
        #     for region in ["north_america", "europe", "japan", "asia_pacific_ex_japan"]
        # ],
    ]

    def matches_any(s: Source, selection: list[str]) -> bool:
        return any(s.name == pattern or s.id == pattern for pattern in selection)

    if config.selected is not None:
        sources = [s for s in sources if matches_any(s, config.selected)]
    if config.skip is not None:
        sources = [s for s in sources if not matches_any(s, config.skip)]
    return sources


def injester_maxx(config: Config):
    for source in configured_sources(config):
        source.ingest()
