import json
from pathlib import Path

import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model

logger = get_logger(__name__)


def compute_from_source(raw_data_path: str | Path) -> pl.DataFrame:
    """Parse the SEC company_tickers.json file and return a flat DataFrame.

    The source file is a JSON object keyed by sequential integers (which are
    discarded). Each value contains cik_str, ticker, and title.

    Args:
        raw_data_path: Root raw data directory containing company_tickers.json.

    Returns:
        Eager DataFrame with columns:

        - ``cik_str`` – CIK as an integer
        - ``ticker``  – exchange ticker symbol
        - ``title``   – company name
    """

    file_path = Path(raw_data_path) / "company_tickers.json"
    logger.info("Reading company_tickers from %s", file_path)

    data = json.loads(file_path.read_bytes())
    rows = [
        {
            "cik_str": entry.get("cik_str"),
            "ticker": entry.get("ticker"),
            "title": entry.get("title"),
        }
        for entry in data.values()
    ]

    df = pl.from_dicts(
        rows,
        schema={"cik_str": pl.Int64, "ticker": pl.String, "title": pl.String},
    )

    logger.info("Returning company_tickers: %d rows × %d cols", df.height, df.width)
    return df


class CompanyTickersSilver(Model):
    def __init__(self, raw_data_path: str | Path | None = None) -> None:
        super().__init__(name="company_tickers", layer="silver")
        self.raw_data_path = raw_data_path

    def _build(self) -> pl.DataFrame:
        if self.raw_data_path is None:
            raise ValueError("raw_data_path is required to build CompanyTickersSilver")
        return compute_from_source(self.raw_data_path)
