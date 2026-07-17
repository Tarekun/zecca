import json
from datetime import date
from pathlib import Path
import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.company_tickers import CompanyTickersSilver
from etl.transformation.silver.candles_daily import CandlesDailySilver

logger = get_logger(__name__)

_SCHEMA = {
    "cik": pl.Int64,
    "entity_name": pl.String,
    "source_file": pl.String,
    "shares_outstanding_end": pl.String,
    "shares_outstanding_filed": pl.String,
    "shares_outstanding_fp": pl.String,
    "shares_outstanding": pl.Int64,
    "public_float_end": pl.String,
    "public_float_filed": pl.String,
    "non_affiliate_valuation": pl.Int128,
    "earnings_end": pl.String,
    "earnings_filed": pl.String,
    "earnings": pl.Int128,
}
_CHUNK_SIZE = 500
# A full fiscal year is ~365 days; this range tolerates short/long fiscal
# years without matching the quarterly figures also tagged fp="FY" (a 10-K
# tags every fact it discloses — including quarterly comparatives — with its
# own filing period, "FY", regardless of that fact's actual start/end span).
_ANNUAL_DURATION_DAYS = (350, 380)


def _annual_net_income_entries(gaap: dict) -> list[dict]:
    """Returns one NetIncomeLoss entry per distinct fiscal year end date.

    Entries whose start/end span isn't ~1 year are dropped (see
    _ANNUAL_DURATION_DAYS). The same fiscal year is routinely re-disclosed as
    the prior-year comparative in the next filing; when an end date has more
    than one entry, the most recently filed one wins."""

    entries = gaap.get("NetIncomeLoss", {}).get("units", {}).get("USD") or []

    by_end: dict[str, dict] = {}
    for e in entries:
        start, end, filed = e.get("start"), e.get("end"), e.get("filed")
        if start is None or end is None or filed is None:
            continue
        try:
            duration = (date.fromisoformat(end) - date.fromisoformat(start)).days
        except ValueError:
            continue
        if not (_ANNUAL_DURATION_DAYS[0] <= duration <= _ANNUAL_DURATION_DAYS[1]):
            continue
        if end not in by_end or filed > by_end[end]["filed"]:
            by_end[end] = e

    return list(by_end.values())


def _extract_rows(file_path: Path) -> list[dict]:
    """Extract EntityCommonStockSharesOutstanding, EntityPublicFloat, and annual
    NetIncomeLoss rows from one SEC JSON file.

    Always returns at least one row. If any key in the nested path is absent the
    metric-specific columns are null so no file is silently dropped"""

    null_row = {
        "cik": None,
        "entity_name": None,
        "source_file": file_path.name,
        "shares_outstanding_end": None,
        "shares_outstanding_filed": None,
        "shares_outstanding_fp": None,
        "shares_outstanding": None,
        "public_float_end": None,
        "public_float_filed": None,
        "non_affiliate_valuation": None,
        "earnings_end": None,
        "earnings_filed": None,
        "earnings": None,
    }
    try:
        data = json.loads(file_path.read_bytes())
    except Exception as e:
        logger.warning("Could not read %s: %s", file_path.name, e)
        return [null_row]

    cik = data.get("cik")
    entity_name = data.get("entityName")
    common = {"cik": cik, "entity_name": entity_name, "source_file": file_path.name}
    dei = data.get("facts", {}).get("dei", {})
    gaap = data.get("facts", {}).get("us-gaap", {})

    shares_entries = (
        dei.get("EntityCommonStockSharesOutstanding", {}).get("units", {}).get("shares")
        or []
    )
    shares_rows = [
        {
            **common,
            "shares_outstanding_end": e.get("end"),
            "shares_outstanding_filed": e.get("filed"),
            "shares_outstanding_fp": e.get("fp"),
            "shares_outstanding": e.get("val"),
            "public_float_end": None,
            "public_float_filed": None,
            "non_affiliate_valuation": None,
            "earnings_end": None,
            "earnings_filed": None,
            "earnings": None,
        }
        for e in shares_entries
    ]

    float_entries = dei.get("EntityPublicFloat", {}).get("units", {}).get("USD") or []
    float_rows = [
        {
            **common,
            "shares_outstanding_end": None,
            "shares_outstanding_filed": None,
            "shares_outstanding_fp": None,
            "shares_outstanding": None,
            "public_float_end": e.get("end"),
            "public_float_filed": e.get("filed"),
            "non_affiliate_valuation": e.get("val"),
            "earnings_end": None,
            "earnings_filed": None,
            "earnings": None,
        }
        for e in float_entries
    ]

    earnings_rows = [
        {
            **common,
            "shares_outstanding_end": None,
            "shares_outstanding_filed": None,
            "shares_outstanding_fp": None,
            "shares_outstanding": None,
            "public_float_end": None,
            "public_float_filed": None,
            "non_affiliate_valuation": None,
            "earnings_end": e.get("end"),
            "earnings_filed": e.get("filed"),
            "earnings": e.get("val"),
        }
        for e in _annual_net_income_entries(gaap)
    ]

    rows = shares_rows + float_rows + earnings_rows
    if not rows:
        return [{**null_row, "cik": cik, "entity_name": entity_name}]

    return rows


def _enrich_with_float_price(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Join each public float entry with the opening price on its end date and
    compute ``estimated_float_shares = non_affiliate_valuation / open``.

    Rows that have no matching ticker or no candle on that date get a null
    ``estimated_float_shares``.  The ticker column used for the join is not
    kept in the output"""

    try:
        tickers = (
            CompanyTickersSilver()
            .read_from_disk()
            .select(pl.col("cik_str").alias("cik"), pl.col("ticker"))
        )
        prices = (
            CandlesDailySilver("")
            .read_from_disk()
            .select(["timeframe", "symbol", "open"])
            .rename({"timeframe": "public_float_end", "symbol": "ticker"})
        )
    except Exception as e:
        logger.warning(
            "Dependencies not found on disk — estimated_float_shares will be null: %s",
            e,
        )
        return lf.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("estimated_float_shares")
        )

    return (
        lf.join(tickers, on="cik", how="left")
        .join(prices, on=["ticker", "public_float_end"], how="left")
        .with_columns(
            (pl.col("non_affiliate_valuation").cast(pl.Float64) / pl.col("open")).alias(
                "estimated_float_shares"
            )
        )
        .drop(["open"])
    )


def compute_from_source(sec_data_path: str | Path) -> pl.LazyFrame:
    """Parse all SEC company facts JSON files under ``sec_data_path`` and return a
    flat LazyFrame of EntityCommonStockSharesOutstanding, EntityPublicFloat, and
    annual NetIncomeLoss entries.

    Each metric's entries are represented as separate rows; metric-specific columns
    are null on rows belonging to the other metrics.

    When ``dataplatform_root`` is provided the function also reads ``company_tickers``
    and ``candles_daily`` from the silver layer to compute ``estimated_float_shares``
    (``non_affiliate_valuation`` divided by the opening price on the public float end
    date).  If either dependency is unavailable the column is present but null.

    Files are processed sequentially in chunks so only a small window of JSON
    data is held in memory at any time.

    Returns:
        LazyFrame with columns:

        - ``cik``                      – company CIK (integer)
        - ``entity_name``              – from entityName
        - ``source_file``              – originating filename
        - ``shares_outstanding_end``   – period end date for shares outstanding
        - ``shares_outstanding_filed`` – filing date for shares outstanding
        - ``shares_outstanding_fp``    – fiscal period for shares outstanding
        - ``shares_outstanding``       – shares outstanding count
        - ``public_float_end``         – period end date for public float
        - ``public_float_filed``       – filing date for public float
        - ``non_affiliate_valuation``  – public float value in USD
        - ``estimated_float_shares``   – non_affiliate_valuation / open price on public_float_end
        - ``earnings_end``             – fiscal year end date for annual net income
        - ``earnings_filed``           – filing date for annual net income
        - ``earnings``                 – annual NetIncomeLoss value in USD
    """

    sec_dir = Path(sec_data_path)
    json_files = sorted(sec_dir.glob("*.json"))

    chunks = []
    for i in range(0, len(json_files), _CHUNK_SIZE):
        batch = json_files[i : i + _CHUNK_SIZE]
        rows = []
        for file_path in batch:
            rows.extend(_extract_rows(file_path))
        chunks.append(pl.from_dicts(rows, schema=_SCHEMA).lazy())

    lf = pl.concat(chunks).with_columns(
        pl.col("shares_outstanding_end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("shares_outstanding_filed").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("public_float_end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("public_float_filed").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("earnings_end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("earnings_filed").str.to_date(format="%Y-%m-%d", strict=False),
    )
    return _enrich_with_float_price(lf)


class SecCompanyFactsSilver(Model):
    def __init__(
        self,
        sec_data_path: str | Path | None = None,
        dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(
            name="sec_company_facts",
            layer="silver",
            dataplatform_root=dataplatform_root,
        )
        self.sec_data_path = sec_data_path

    def _build(self) -> pl.LazyFrame:
        if self.sec_data_path is None:
            raise ValueError("sec_data_path is required to build SecCompanyFactsSilver")
        return compute_from_source(self.sec_data_path)
