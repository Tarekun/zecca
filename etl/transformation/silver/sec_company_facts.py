import json
from datetime import date
from pathlib import Path
import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import (
    accepted_values,
    is_finite,
    not_empty,
    not_null,
    unique,
)
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
    "earnings_start": pl.String,
    "earnings_end": pl.String,
    "earnings_filed": pl.String,
    "earnings": pl.Int128,
}
_DATE_COLUMNS = [
    "shares_outstanding_end",
    "shares_outstanding_filed",
    "public_float_end",
    "public_float_filed",
    "earnings_start",
    "earnings_end",
    "earnings_filed",
]


def _shares_outstanding_rows(dei: dict) -> list[dict]:
    """One row per EntityCommonStockSharesOutstanding entry, with only its own
    (non-common) columns set."""

    entries = (
        dei.get("EntityCommonStockSharesOutstanding", {}).get("units", {}).get("shares")
        or []
    )
    return [
        {
            "shares_outstanding_end": e.get("end"),
            "shares_outstanding_filed": e.get("filed"),
            "shares_outstanding_fp": e.get("fp"),
            "shares_outstanding": e.get("val"),
        }
        for e in entries
    ]


def _public_float_rows(dei: dict) -> list[dict]:
    """One row per EntityPublicFloat entry, with only its own (non-common)
    columns set."""

    entries = dei.get("EntityPublicFloat", {}).get("units", {}).get("USD") or []
    return [
        {
            "public_float_end": e.get("end"),
            "public_float_filed": e.get("filed"),
            "non_affiliate_valuation": e.get("val"),
        }
        for e in entries
    ]


def _earnings_rows(gaap: dict) -> list[dict]:
    """One row per annual NetIncomeLoss entry (see _annual_net_income_entries),
    with only its own (non-common) columns set."""

    entries = gaap.get("NetIncomeLoss", {}).get("units", {}).get("USD") or []

    by_end: dict[str, dict] = {}
    for e in entries:
        start, end, filed = e.get("start"), e.get("end"), e.get("filed")
        if start is None or end is None or filed is None:
            continue

        # A full fiscal year is ~365 days; this range tolerates short/long fiscal
        # years without matching the quarterly figures also tagged fp="FY" (a 10-K
        # tags every fact it discloses — including quarterly comparatives — with its
        # own filing period, "FY", regardless of that fact's actual start/end span).
        duration = (date.fromisoformat(end) - date.fromisoformat(start)).days
        if not 350 <= duration <= 380:
            continue
        if end not in by_end or filed > by_end[end]["filed"]:
            by_end[end] = e

    return [
        {
            "earnings_start": e.get("start"),
            "earnings_end": e.get("end"),
            "earnings_filed": e.get("filed"),
            "earnings": e.get("val"),
        }
        for e in list(by_end.values())
    ]


def _cik_from_filename(file_path: Path) -> int | None:
    digits = file_path.stem.removeprefix("CIK").lstrip("0")
    return int(digits) if digits else None


def _extract_rows_from_file(file_path: Path) -> pl.DataFrame:
    """Extracts all EntityCommonStockSharesOutstanding, EntityPublicFloat, and
    annual NetIncomeLoss rows from one SEC JSON file into a DataFrame following
    _SCHEMA.

    Always returns at least one row so no file is silently dropped: if the
    file has none of the three facts, the row has only
    cik/entity_name/source_file set."""

    data = json.loads(file_path.read_bytes())
    dei = data.get("facts", {}).get("dei", {})
    gaap = data.get("facts", {}).get("us-gaap", {})
    facts = (
        _shares_outstanding_rows(dei) + _public_float_rows(dei) + _earnings_rows(gaap)
    )

    common = {
        "cik": data.get("cik") or _cik_from_filename(file_path),
        "entity_name": data.get("entityName"),
        "source_file": file_path.name,
    }
    rows = [{**common, **row} for row in facts] or [common]

    return pl.from_dicts(rows, schema=_SCHEMA)


def _enrich_with_float_price(df: pl.DataFrame) -> pl.DataFrame:
    """Joins each public float entry with the opening price on its end date and
    computes ``estimated_float_shares = non_affiliate_valuation / open``.

    Rows that have no matching ticker or no candle on that date get a null
    ``estimated_float_shares``. The ticker column used for the join is not
    kept in the output."""

    try:
        tickers = (
            CompanyTickersSilver()
            .read_from_disk()
            .select(pl.col("cik_str").alias("cik"), pl.col("ticker"))
            .collect()
        )
        prices = (
            CandlesDailySilver("")
            .read_from_disk()
            .select(["timeframe", "symbol", "open"])
            .rename({"timeframe": "public_float_end", "symbol": "ticker"})
            .collect()
        )
    except Exception as e:
        logger.warning(
            "Dependencies not found on disk — estimated_float_shares will be null: %s",
            e,
        )
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("estimated_float_shares")
        )

    return (
        df.join(tickers, on="cik", how="left")
        .join(prices, on=["ticker", "public_float_end"], how="left")
        .with_columns(
            (pl.col("non_affiliate_valuation").cast(pl.Float64) / pl.col("open")).alias(
                "estimated_float_shares"
            )
        )
        .drop(["open"])
    )


def compute_from_source(sec_data_path: Path) -> pl.DataFrame:
    """Parse all SEC company facts JSON files under ``sec_data_path`` into a
    flat DataFrame of EntityCommonStockSharesOutstanding, EntityPublicFloat,
    and annual NetIncomeLoss entries (see ``_extract_rows_from_file``), then
    estimate each public float entry's implied share count (see
    ``_enrich_with_float_price``).

    Each metric's entries are represented as separate rows; metric-specific
    columns are null on rows belonging to the other metrics.

    Returns:
        DataFrame with columns:

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
        - ``earnings_start``           – fiscal year start date for annual net income
        - ``earnings_end``             – fiscal year end date for annual net income
        - ``earnings_filed``           – filing date for annual net income
        - ``earnings``                 – annual NetIncomeLoss value in USD
    """

    json_files = sorted(sec_data_path.glob("*.json"))
    df = pl.concat([_extract_rows_from_file(f) for f in json_files]).with_columns(
        [pl.col(c).str.to_date(format="%Y-%m-%d", strict=False) for c in _DATE_COLUMNS]
    )
    return _enrich_with_float_price(df)


class SecCompanyFactsSilver(Model):
    def __init__(
        self,
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(
            name="sec_company_facts",
            layer="silver",
            dataplatform_root=dataplatform_root,
            quality_checks=[
                not_empty(),
                is_finite(["estimated_float_shares"]),
                not_null(["cik", "source_file"]),
                test_each_cik_has_at_least_one_metric,
                test_cik_count_matches_file_count,
                accepted_values(
                    "shares_outstanding_fp",
                    [
                        # companies file quarterly
                        "Q1",
                        "Q2",
                        "Q3",
                        "Q4",
                        "FY",  # fiscal year
                        # the following appear in form transition/foreign filer
                        # file every trimester
                        "T1",
                        "T2",
                        "T3",
                        # file twice year
                        "H1",
                        "H2",
                        "CY",  # calendar year,
                    ],
                ),
            ],
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source(Path(self.dataplatform_root) / "raw" / "sec").lazy()


def test_each_cik_has_at_least_one_metric(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Every CIK in the silver model must have at least one row with a non-null
    shares_outstanding or non_affiliate_valuation.

    CIKs with no metric data are written to
    dataplatform/test_outputs/sec_company_facts_missing_val.csv for inspection.
    """
    metrics_lf = lf
    ciks_with_data = (
        metrics_lf.filter(
            pl.col("shares_outstanding").is_not_null()
            | pl.col("non_affiliate_valuation").is_not_null()
        )
        .select("cik")
        .unique()
    )
    all_ciks = metrics_lf.select("cik").unique()

    missing_val = all_ciks.join(ciks_with_data, on="cik", how="anti").sort("cik")
    return missing_val


def test_cik_count_matches_file_count(lf: pl.LazyFrame) -> pl.LazyFrame:
    """The number of distinct CIK values in the silver model must equal the number
    of source JSON files under dataplatform/raw/sec — one row (possibly null) per file.
    """
    file_count = len(
        list(Path(f"./{DEFAULT_DATAPLATFORM_ROOT}/raw/sec").glob("*.json"))
    )
    distinct_ciks = lf.select(pl.col("cik").n_unique()).collect().item()

    assert distinct_ciks == file_count, (
        f"Expected {file_count} distinct CIK values (one per source file) "
        f"but found {distinct_ciks}."
    )
    return pl.LazyFrame()
