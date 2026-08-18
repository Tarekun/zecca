from datetime import date
import polars as pl

from etl.transformation.quality_checks import (
    column_comparison,
    no_gaps,
    not_empty,
    not_null,
    unique,
)
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver


def _pad_series(
    lf: pl.LazyFrame, end_col: str, filed_col: str, today: date
) -> pl.LazyFrame:
    """Expand a single metric's time series to one row per calendar day per CIK.

    An entry's value is only actually known once it's filed with the SEC, so
    padding forward from ``end_col`` (the reported period) would make the value
    available before the public could have known it — a look-ahead bias.
    Instead, for each CIK the rows are sorted by ``filed_col`` and padded
    forward from there: each entry's value covers every date from ``filed_col``
    up to (but not including) the next entry's ``filed_col`` date. The most
    recent entry per CIK is padded forward to ``today``. Rows with a null
    ``end_col`` or ``filed_col`` are dropped.

    Args:
        lf:        LazyFrame containing at least ``cik``, ``end_col``, and
                   ``filed_col``.
        end_col:   Name of the Date column giving the metric's reported period end.
        filed_col: Name of the Date column giving the metric's filing date; this
                   is what ``reference_date`` is actually anchored to.
        today:     Ceiling date for the most recent entry (exclusive upper bound).

    Returns:
        LazyFrame with the same columns as ``lf`` (``end_col`` and ``filed_col``
        included, reflecting the currently active entry), plus a
        ``reference_date`` (Date) column.
    """
    return (
        lf.filter(pl.col(end_col).is_not_null() & pl.col(filed_col).is_not_null())
        .sort(["cik", filed_col, end_col])
        .with_columns(pl.col(filed_col).shift(-1).over("cik").alias("_next_filed"))
        .with_columns(
            pl.when(pl.col("_next_filed").is_null())
            .then(pl.lit(today))
            .otherwise(pl.col("_next_filed") - pl.duration(days=1))
            .cast(pl.Date)
            .alias("valid_until")
        )
        .drop("_next_filed")
        # When two entries for the same CIK share the identical filed_col date
        # (e.g. several comparative periods disclosed in one filing),
        # valid_until ends up one day *before* filed_col for the earlier one,
        # since it's superseded the instant its same-day successor is filed.
        # date_ranges() on such an inverted range returns an empty list, and
        # exploding an empty list yields one row with a null reference_date
        # instead of zero rows -- so these entries must be dropped up front.
        .filter(pl.col("valid_until") >= pl.col(filed_col))
        .with_columns(
            pl.date_ranges(
                pl.col(filed_col), pl.col("valid_until"), interval="1d"
            ).alias("reference_date")
        )
        .explode("reference_date")
        .drop("valid_until")
    )


def compute_from_source() -> pl.LazyFrame:
    """Read sec_company_facts from the silver layer and expand to one row per
    (cik, reference_date), padding each metric's time series independently.

    EntityCommonStockSharesOutstanding, EntityPublicFloat, and annual
    NetIncomeLoss are padded separately so that entries from one metric never
    influence the forward-fill boundaries of another.  Each series is anchored
    on its own filing date rather than its reported period end (see
    ``_pad_series``), so ``reference_date`` never precedes the date a value was
    actually made public.  The three padded series are then outer-joined on
    (cik, reference_date).

    Args:
        dataplatform_root: Root of the dataplatform directory (e.g. "./dataplatform").

    Returns:
        LazyFrame with columns:

        - ``cik``                      – company CIK (integer)
        - ``entity_name``              – company name
        - ``ticker``                   – exchange ticker (null when not in company_tickers)
        - ``reference_date``           – calendar date (Date)
        - ``last_filed``               – filing date of the most recently filed of the
                                          three metrics' currently active entries
        - ``shares_outstanding_fp``    – fiscal period of the active shares report
        - ``shares_outstanding``       – shares outstanding on ``reference_date``
        - ``shares_outstanding_end``   – period end date of the active shares report
        - ``non_affiliate_valuation``  – public float in USD on ``reference_date``
        - ``estimated_float_shares``   – non_affiliate_valuation / open price on the filing date
        - ``public_float_end``         – period end date of the active public float report
        - ``earnings``                 – most recently reported annual net income (USD)
        - ``earnings_start``           – fiscal year start date of the active net income report
        - ``earnings_end``             – fiscal year end date of the active net income report
    """

    today = date.today()
    lf = (
        SecCompanyFactsSilver()
        .read_from_disk()
        .filter(pl.col("cik").is_not_null())
        .drop("source_file")
    )

    shares_padded = _pad_series(
        lf.select(
            [
                "cik",
                "shares_outstanding_end",
                "shares_outstanding_filed",
                "shares_outstanding_fp",
                "shares_outstanding",
            ]
        ),
        end_col="shares_outstanding_end",
        filed_col="shares_outstanding_filed",
        today=today,
    )
    float_padded = _pad_series(
        lf.select(
            [
                "cik",
                "public_float_end",
                "public_float_filed",
                "non_affiliate_valuation",
                "estimated_float_shares",
            ]
        ),
        end_col="public_float_end",
        filed_col="public_float_filed",
        today=today,
    )
    earnings_padded = _pad_series(
        lf.select(
            ["cik", "earnings_start", "earnings_end", "earnings_filed", "earnings"]
        ),
        end_col="earnings_end",
        filed_col="earnings_filed",
        today=today,
    )

    combined = (
        shares_padded.join(
            float_padded,
            on=["cik", "reference_date"],
            how="full",
            coalesce=True,
        )
        .join(
            earnings_padded,
            on=["cik", "reference_date"],
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.max_horizontal(
                "shares_outstanding_filed", "public_float_filed", "earnings_filed"
            ).alias("last_filed")
        )
        .drop(["shares_outstanding_filed", "public_float_filed", "earnings_filed"])
    )
    entity_names = lf.select(["cik", "entity_name", "ticker"]).unique(
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
                "last_filed",
                "shares_outstanding_fp",
                "shares_outstanding",
                "shares_outstanding_end",
                "non_affiliate_valuation",
                "estimated_float_shares",
                "public_float_end",
                "earnings",
                "earnings_start",
                "earnings_end",
            ]
        )
        .sort(["cik", "reference_date"])
    )


class SecCompanyFactsPaddedSilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="sec_company_facts_padded",
            layer="silver",
            quality_checks=[
                not_empty(),
                not_null(["cik", "reference_date", "last_filed"]),
                not_null(["ticker"]),
                unique(["cik", "reference_date"]),
                column_comparison("reference_date", ">=", "last_filed"),
                no_gaps("reference_date", group_by="cik"),
                test_every_filing_filed_date_present_as_reference_date,
            ],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source()


def test_every_filing_filed_date_present_as_reference_date(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Every (cik, filed_date) pair for each metric in sec_company_facts must
    appear as a (cik, reference_date) row in sec_company_facts_padded — padding
    only fills the gaps between filings, it must never drop a filing's own date.

    reference_date is anchored on the filing date (not the reported period end)
    since that's when a value actually becomes public; see _pad_series.

    Missing (cik, filed_date, metric) triples are written to
    dataplatform/test_outputs/sec_company_facts_padded_missing_filing_dates.csv.
    """

    reference_dates = lf.select(["cik", "reference_date"]).unique()
    facts_lf = SecCompanyFactsSilver().read_from_disk()

    missing_frames = []
    for filed_col in [
        "shares_outstanding_filed",
        "public_float_filed",
        "earnings_filed",
    ]:
        filings = (
            facts_lf.select(["cik", filed_col])
            .filter(pl.col("cik").is_not_null() & pl.col(filed_col).is_not_null())
            .unique()
            .rename({filed_col: "reference_date"})
        )
        missing = (
            filings.join(reference_dates, on=["cik", "reference_date"], how="anti")
            .with_columns(pl.lit(filed_col).alias("metric"))
            .collect()
        )
        if missing.height > 0:
            missing_frames.append(missing)

    if missing_frames:
        return pl.concat(missing_frames)
    else:
        return pl.LazyFrame()
