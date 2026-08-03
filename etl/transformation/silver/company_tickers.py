import json
from pathlib import Path
import polars as pl

from etl.ingestion.sec import SecTickers
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import matches_regex, not_empty, not_null, unique


def compute_from_source(raw_data_path: Path) -> pl.LazyFrame:
    """Parse the SEC company_tickers.json file and return a flat LazyFrame.

    The source file is a JSON object keyed by sequential integers (which are
    discarded). Each value contains cik_str, ticker, and title.

    Args:
        raw_data_path: Root raw data directory containing company_tickers.json.

    Returns:
        LazyFrame with columns:

        - ``cik_str`` – CIK as an integer
        - ``ticker``  – exchange ticker symbol
        - ``title``   – company name
    """

    data = SecTickers().read_from_disk()[0]
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

    return df.lazy()


class CompanyTickersSilver(Model):
    def __init__(
        self,
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(
            name="company_tickers",
            layer="silver",
            quality_checks=[
                not_empty(),
                not_null(["cik_str", "ticker"]),
                # note that cik_str can appear more than once associated with different tickers
                # example: cik=1652044, ticker=GOOGL,GOOG,GOOGM,GOOGN
                unique(["cik_str", "ticker"]),
                unique(["ticker"]),
                matches_regex("ticker", r"[A-Za-z0-9.\-]+"),
            ],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source(Path(self.dataplatform_root) / "raw")
