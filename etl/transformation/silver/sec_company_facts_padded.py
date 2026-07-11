from datetime import date
from pathlib import Path

import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver

logger = get_logger(__name__)


def _pad_series(df: pl.DataFrame, end_col: str, today: date) -> pl.DataFrame:
    """Expand a single metric's time series to one row per calendar day per CIK.

    For each CIK the rows are sorted by ``end_col`` and padded forward: each
    entry's value covers every date from ``end_col`` up to (but not including)
    the next entry's ``end_col`` date.  The most recent entry per CIK is padded
    forward to ``today``.  Rows with a null ``end_col`` are dropped.

    Args:
        df:      DataFrame containing at least ``cik`` and ``end_col``.
        end_col: Name of the Date column that anchors this metric's time series.
        today:   Ceiling date for the most recent entry (exclusive upper bound).

    Returns:
        DataFrame with the same columns as ``df`` minus ``end_col``, plus a
        ``reference_date`` (Date) column.
    """
    return (
        df.filter(pl.col(end_col).is_not_null())
        .sort(["cik", end_col])
        .with_columns(pl.col(end_col).shift(-1).over("cik").alias("_next_end"))
        .with_columns(
            pl.when(pl.col("_next_end").is_null())
            .then(pl.lit(today))
            .otherwise(pl.col("_next_end") - pl.duration(days=1))
            .cast(pl.Date)
            .alias("valid_until")
        )
        .drop("_next_end")
        .with_columns(
            pl.date_ranges(pl.col(end_col), pl.col("valid_until"), interval="1d").alias(
                "reference_date"
            )
        )
        .explode("reference_date")
        .drop(["valid_until", end_col])
    )


def compute_from_source() -> pl.DataFrame:
    """Read sec_company_facts from the silver layer and expand to one row per
    (cik, reference_date), padding each metric's time series independently.

    EntityCommonStockSharesOutstanding and EntityPublicFloat are padded
    separately so that entries from one metric never influence the forward-fill
    boundaries of the other.  The two padded series are then outer-joined on
    (cik, reference_date).

    Args:
        dataplatform_root: Root of the dataplatform directory (e.g. "./dataplatform").

    Returns:
        Eager DataFrame with columns:

        - ``cik``                     – company CIK (integer)
        - ``entity_name``             – company name
        - ``ticker``                  – exchange ticker (null when not in company_tickers)
        - ``reference_date``          – calendar date (Date)
        - ``shares_outstanding_fp``   – fiscal period of the active shares report
        - ``shares_outstanding``      – shares outstanding on ``reference_date``
        - ``non_affiliate_valuation``  – public float in USD on ``reference_date``
        - ``estimated_float_shares``   – non_affiliate_valuation / open price on the filing date
    """

    today = date.today()
    df = (
        SecCompanyFactsSilver()
        .load_from_disk()
        .filter(pl.col("cik").is_not_null())
        .drop("source_file")
    )

    shares_padded = _pad_series(
        df.select(
            [
                "cik",
                "shares_outstanding_end",
                "shares_outstanding_fp",
                "shares_outstanding",
            ]
        ),
        end_col="shares_outstanding_end",
        today=today,
    )
    float_padded = _pad_series(
        df.select(
            [
                "cik",
                "public_float_end",
                "non_affiliate_valuation",
                "estimated_float_shares",
            ]
        ),
        end_col="public_float_end",
        today=today,
    )

    combined = shares_padded.join(
        float_padded,
        on=["cik", "reference_date"],
        how="full",
        coalesce=True,
    )
    entity_names = df.select(["cik", "entity_name", "ticker"]).unique(
        subset=["cik"], keep="first"
    )

    return (
        combined.join(entity_names, on="cik", how="left")
        .select(
            [
                "cik",
                "entity_name",
                "ticker",
                "reference_date",
                "shares_outstanding_fp",
                "shares_outstanding",
                "non_affiliate_valuation",
                "estimated_float_shares",
            ]
        )
        .sort(["cik", "reference_date"])
    )


class SecCompanyFactsPaddedSilver(Model):
    def __init__(self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="sec_company_facts_padded",
            layer="silver",
            dataplatform_root=dataplatform_root,
        )
        self.configure_dependencies([SecCompanyFactsSilver])

    def _build(self) -> pl.DataFrame:
        return compute_from_source()
