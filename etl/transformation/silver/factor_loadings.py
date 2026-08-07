from datetime import date
from dateutil.relativedelta import relativedelta
import numpy as np
import polars as pl

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import not_empty, not_null, unique, is_finite
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.fama_french_5 import FamaFrench5Silver

FIRST_DATE = date(2000, 1, 1)
FINAL_DATE = date(2026, 1, 1)
# TODO: properly discuss what values to set here
TIME_WINDOW_MONTHS = 36  # 3-year estimation window for factor exposures
TIME_SHIFT_MONTHS = 3  # roll the window forward a quarter between estimates
MAX_MISSING_RATIO = 0.1

FF3_FACTORS = ["Mkt-RF", "SMB", "HML"]
NORTH_AMERICA_REGION = "north_america"
LOADING_COLUMNS = ["alpha", "beta_mkt_rf", "beta_smb", "beta_hml"]


def load_scaled_factors(
    dataplatform_root: str, start_date: date, end_date: date
) -> pl.LazyFrame:
    """North-america FF3 factors (+ RF) over [start_date, end_date), as
    `timeframe`/`Mkt-RF`/`SMB`/`HML`/`RF`.

    FF5 factors are published in percentage points (e.g. 0.30 means 0.30%);
    they're divided by 100 here to land on the same decimal scale as
    candles_daily's log_return_1d.
    """
    return (
        FamaFrench5Silver(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .filter(
            pl.col("region") == NORTH_AMERICA_REGION,
            pl.col("date") >= start_date,
            pl.col("date") < end_date,
        )
        .rename({"date": "timeframe"})
        .select(["timeframe", *FF3_FACTORS, "RF"])
        .with_columns([(pl.col(c) / 100).alias(c) for c in [*FF3_FACTORS, "RF"]])
    )


def load_excess_returns(
    dataplatform_root: str,
    start_date: date,
    end_date: date,
    factors_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join the full stock universe's returns with `factors_lf` over
    [start_date, end_date), returning one row per (symbol, timeframe) with the
    stock's excess return (log_return_1d minus the risk-free rate).

    No eligibility filtering happens here -- every symbol in candles_daily is
    a candidate; `_impute_missing_returns` is the only filter, dropping a
    symbol from a window if too much of its return series is missing.
    """
    candles = CandlesDailySilver(dataplatform_root=dataplatform_root).read_from_disk()

    stock_returns = candles.filter(
        pl.col("timeframe") >= start_date, pl.col("timeframe") < end_date
    ).select(["symbol", "timeframe", "log_return_1d"])

    return (
        stock_returns.join(
            factors_lf.select(["timeframe", "RF"]), on="timeframe", how="inner"
        )
        .with_columns((pl.col("log_return_1d") - pl.col("RF")).alias("excess_return"))
        .select(["symbol", "timeframe", "excess_return"])
    )


def _impute_missing_returns(
    lf: pl.LazyFrame, max_missing_ratio: float = MAX_MISSING_RATIO
) -> pl.LazyFrame:
    """Drop symbols whose excess_return is missing (absent, null, or
    non-finite) on more than `max_missing_ratio` of the days in range. Symbols
    within the threshold are kept in full, with their missing days imputed
    from that day's cross-sectional mean excess return, so the panel used for
    the regression below has no gaps (mirrors symbol_embeddings.drop_incomplete_symbols).
    """

    lf = lf.unique(subset=["symbol", "timeframe"], keep="first")

    n_days = lf.select("timeframe").unique().collect().height
    is_valid = (
        pl.col("excess_return").is_not_null() & pl.col("excess_return").is_finite()
    )

    kept_symbols = (
        lf.group_by("symbol")
        .agg(is_valid.sum().alias("n_valid"))
        .with_columns((1 - (pl.col("n_valid") / n_days)).alias("missing_ratio"))
        .filter(pl.col("missing_ratio") <= max_missing_ratio)
        .select("symbol")
    )
    market_mean_by_day = (
        lf.filter(is_valid)
        .group_by("timeframe")
        .agg(pl.col("excess_return").mean().alias("market_mean"))
    )

    full_grid = kept_symbols.join(market_mean_by_day, how="cross")
    return (
        full_grid.join(lf, on=["symbol", "timeframe"], how="left")
        .with_columns(
            pl.when(is_valid)
            .then(pl.col("excess_return"))
            .otherwise(pl.col("market_mean"))
            .alias("excess_return")
        )
        .select("symbol", "timeframe", "excess_return")
    )


def estimate_ols_loadings(
    returns_lf: pl.LazyFrame, factors_lf: pl.LazyFrame
) -> pl.DataFrame:
    """Estimates one Fama-French three-factor loading set (alpha, beta_mkt_rf,
    beta_smb, beta_hml) per symbol via Ordinary Least Squares: for each
    symbol, regress its daily excess return on [1, Mkt-RF, SMB, HML] over the
    window.

    Every symbol shares the same set of trading days (`_impute_missing_returns`
    already made the panel dense), so every regression shares the same design
    matrix X. Rather than looping per symbol, the whole (n_days, n_symbols)
    response matrix is solved in one closed-form least-squares problem via
    `numpy.linalg.lstsq` (SVD-based, numerically stable) — this is the
    standard OLS estimator for asset-pricing factor loadings (Fama & French,
    1993), applied here to every symbol at once since lstsq accepts a matrix
    of right-hand sides.
    """
    panel = (
        _impute_missing_returns(returns_lf)
        .collect()
        .pivot(on="timeframe", index="symbol", values="excess_return")
        .sort("symbol")
    )
    # pivot doesn't guarantee chronological column order, so realign explicitly
    day_columns = sorted(c for c in panel.columns if c != "symbol")

    factors_by_day = (
        factors_lf.with_columns(pl.col("timeframe").cast(pl.String).alias("_day"))
        .sort("_day")
        .collect()
    )
    # a trading day present for stocks but absent from the factor file (or
    # vice versa) can't be regressed on -- keep only days present in both
    day_columns = [d for d in day_columns if d in set(factors_by_day["_day"])]
    panel = panel.select(["symbol", *day_columns])
    factors_by_day = factors_by_day.filter(pl.col("_day").is_in(day_columns))

    symbols = panel["symbol"].to_list()
    Y = panel.select(day_columns).to_numpy().T  # (n_days, n_symbols)
    X = factors_by_day.select(FF3_FACTORS).to_numpy()  # (n_days, 3)
    X_design = np.column_stack([np.ones(X.shape[0]), X])  # (n_days, 4): [1, factors]

    loadings, _residuals, _rank, _singular_values = np.linalg.lstsq(
        X_design, Y, rcond=None
    )  # (4, n_symbols): rows are [alpha, beta_mkt_rf, beta_smb, beta_hml]

    return pl.DataFrame(
        {
            "symbol": symbols,
            **{col: loadings[i] for i, col in enumerate(LOADING_COLUMNS)},
        }
    )


def iterate_ols_by_rolling(dataplatform_root: str) -> dict[date, pl.DataFrame]:
    """Iterates OLS factor-loading estimation over rolling TIME_WINDOW_MONTHS
    windows shifted by TIME_SHIFT_MONTHS, returning one DataFrame of
    (symbol, alpha, beta_*) per window, indexed by the window's end date
    (the first day not covered by that window)."""

    loadings_by_window = {}

    start_date = FIRST_DATE
    end_date = start_date + relativedelta(months=TIME_WINDOW_MONTHS)

    while end_date <= FINAL_DATE:
        factors_lf = load_scaled_factors(dataplatform_root, start_date, end_date)
        returns_lf = load_excess_returns(
            dataplatform_root, start_date, end_date, factors_lf
        )
        # candles_daily's history doesn't reach back to FIRST_DATE yet, so early
        # windows have no data at all -- skip rather than divide by zero below
        if returns_lf.select(pl.len()).collect().item() > 0:
            loadings = estimate_ols_loadings(returns_lf, factors_lf)
            if loadings.height > 0:
                loadings_by_window[end_date] = loadings

        start_date = start_date + relativedelta(months=TIME_SHIFT_MONTHS)
        end_date = start_date + relativedelta(months=TIME_WINDOW_MONTHS)

    return loadings_by_window


class FactorLoadingsSilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="factor_loadings",
            layer="silver",
            quality_checks=[
                not_empty(),
                not_null(["not_before", "symbol", *LOADING_COLUMNS]),
                is_finite(LOADING_COLUMNS),
                unique(["not_before", "symbol"]),
            ],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        loadings_by_window = iterate_ols_by_rolling(str(self.dataplatform_root))

        frames = [
            loadings.with_columns(pl.lit(not_before).alias("not_before"))
            for not_before, loadings in loadings_by_window.items()
        ]
        out = pl.concat(frames, how="vertical").sort(["not_before", "symbol"])
        return out.lazy()
