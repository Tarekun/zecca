from datetime import date
from dateutil.relativedelta import relativedelta
import polars as pl
import numpy as np
from scipy.linalg import orthogonal_procrustes
from sklearn.decomposition import TruncatedSVD

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

FIRST_DATE = date(2000, 1, 1)
FINAL_DATE = date(2026, 1, 1)
# TODO: properly discuss what values to set here
TIME_WINDOW_MONTHS = 12
TIME_SHIFT_MONTHS = 6
DEFAULT_EMBEDDING_SIZE = 16
SEED = 16 * 29


def load_log_returns(
    dataplatform_root: str, start_date: date, end_date: date
) -> pl.LazyFrame:
    """Scan CandlesDailySilver from disk, restricted to [start_date, end_date)
    and to the symbol/timeframe/log_return_1d columns."""

    model = CandlesDailySilver(
        yfinance_data_path="", dataplatform_root=dataplatform_root
    )
    return (
        model.read_from_disk()
        .filter(pl.col("timeframe") >= start_date, pl.col("timeframe") < end_date)
        .select("symbol", "timeframe", "log_return_1d")
    )


def drop_incomplete_symbols(
    lf: pl.LazyFrame, max_missing_ratio: float = 0.1
) -> pl.LazyFrame:
    """Drop symbols whose log_return_1d is missing (absent, null, or non-finite)
    on more than `max_missing_ratio` of the days in range. Symbols within the
    threshold are kept in full, with their missing days imputed from that
    day's cross-sectional mean return across the rest of the market, so the
    later pivot still has no gaps."""

    # candles_daily has occasional duplicate (symbol, timeframe) rows; collapse
    # them first so the grid below has at most one value per cell
    lf = lf.unique(subset=["symbol", "timeframe"], keep="first")

    n_days = lf.select("timeframe").unique().collect().height
    is_valid = (
        pl.col("log_return_1d").is_not_null() & pl.col("log_return_1d").is_finite()
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
        .agg(pl.col("log_return_1d").mean().alias("market_mean"))
    )

    # cross join gives a dense symbol x day grid, so entirely-absent rows
    # (not just null ones) also get a slot to impute into. Restrict to days that
    # have a market mean at all -- a day with zero valid entries market-wide
    # (e.g. the very first day of the whole history, where log_return_1d is
    # undefined for everyone) can't be imputed and is dropped instead.
    full_grid = kept_symbols.join(market_mean_by_day, how="cross")

    return (
        full_grid.join(lf, on=["symbol", "timeframe"], how="left")
        .with_columns(
            pl.when(is_valid)
            .then(pl.col("log_return_1d"))
            .otherwise(pl.col("market_mean"))
            .alias("log_return_1d")
        )
        .select("symbol", "timeframe", "log_return_1d")
    )


def iterate_svd_by_rolling(
    embedding_size: int, dataplatform_root: str
) -> dict[date, dict[str, np.ndarray]]:
    """Iterates a TruncatedSVD over over time windows of `TIME_WINDOW_MONTHS` and returns
    all of the computed embeddings, indexed by the end date (first day not used) of the window
    """

    symbol_embeddings = {}

    start_date = FIRST_DATE
    end_date = start_date + relativedelta(months=TIME_WINDOW_MONTHS)

    while end_date <= FINAL_DATE:
        lf = load_log_returns(dataplatform_root, start_date, end_date)
        df = drop_incomplete_symbols(lf).collect()
        panel = df.pivot(on="timeframe", index="symbol", values="log_return_1d").sort(
            "symbol"
        )
        # pivot doesn't guarantee chronological column order, so realign explicitly
        day_columns = sorted(c for c in panel.columns if c != "symbol")
        panel = panel.select(["symbol", *day_columns])

        symbols = panel["symbol"].to_list()
        R = panel.select(day_columns).to_numpy()
        # standardize EACH SYMBOL's series (across time) so co-movement, not raw
        # volatility, drives the factors -- correlation- rather than covariance-based.
        R = (R - R.mean(axis=1, keepdims=True)) / (R.std(axis=1, keepdims=True) + 1e-8)

        embeddings = TruncatedSVD(
            n_components=embedding_size, random_state=SEED
        ).fit_transform(R)
        symbol_to_vec = dict(zip(symbols, embeddings))
        symbol_embeddings[end_date] = symbol_to_vec

        start_date = start_date + relativedelta(months=TIME_SHIFT_MONTHS)
        end_date = start_date + relativedelta(months=TIME_WINDOW_MONTHS)

    return symbol_embeddings


def rotate_embedding_to_latest(
    symbol_embeddings: dict[date, dict[str, np.ndarray]],
    min_shared: int | None = None,
) -> dict[date, dict[str, np.ndarray]]:
    """Align every period's embedding onto the latest period's basis via orthogonal
    Procrustes, so a symbol's vector is comparable across dates.

    SVD bases are defined only up to rotation/reflection, so raw per-period
    embeddings sit in incompatible coordinate frames. We anchor on the most recent
    period and walk backwards, aligning each period to its already-aligned successor
    (adjacent rolling windows share the most symbols -> well-conditioned rotation,
    which then composes back to the canonical frame)"""
    if not symbol_embeddings:
        return {}

    dates = sorted(symbol_embeddings)  # ascending; last is canonical
    dim = len(next(iter(symbol_embeddings[dates[-1]].values())))
    if min_shared is None:
        min_shared = dim  # >= dim to pin a rotation; more is better

    def as_arrays(d: date) -> dict[str, np.ndarray]:
        return {s: np.asarray(v, dtype=float) for s, v in symbol_embeddings[d].items()}

    aligned: dict[date, dict[str, np.ndarray]] = {dates[-1]: as_arrays(dates[-1])}

    for i in range(len(dates) - 2, -1, -1):
        cur = as_arrays(dates[i])
        # for direct-to-canonical instead of chaining, use: ref = aligned[dates[-1]]
        ref = aligned[dates[i + 1]]  # already in canonical frame
        shared = sorted(cur.keys() & ref.keys())
        if len(shared) < min_shared:
            raise Exception(
                f"Dates {dates[i]} and {dates[i+1]} only have {len(shared)} shared symbols (<{min_shared}). Procrustes cannot be applied"
            )

        A = np.vstack([cur[s] for s in shared])  # this period, raw
        B = np.vstack([ref[s] for s in shared])  # successor, already aligned
        R, _ = orthogonal_procrustes(A, B)  # minimizes ||A @ R - B||, R orthogonal

        # apply to ALL symbols in period
        aligned[dates[i]] = {s: v @ R for s, v in cur.items()}

    return aligned


class SymbolEmbeddingsSilver(Model):
    def __init__(
        self,
        embedding_size=DEFAULT_EMBEDDING_SIZE,
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(
            name="symbol_embeddings",
            layer="silver",
            partitioning_columns=["not_before"],
            dataplatform_root=dataplatform_root,
        )
        self.embedding_size = embedding_size

    def _build(self) -> pl.LazyFrame:
        symbol_embeddings = iterate_svd_by_rolling(
            self.embedding_size, str(self.dataplatform_root)
        )
        symbol_embeddings = rotate_embedding_to_latest(symbol_embeddings)

        frames = []
        for not_before, sym_to_vec in symbol_embeddings.items():
            symbols = list(sym_to_vec.keys())
            mat = np.vstack(
                [np.asarray(sym_to_vec[s], dtype=float) for s in symbols]
            )  # (n, k)

            data: dict[str, object] = {
                "not_before": [not_before] * len(symbols),
                "symbol": symbols,
            }
            for j in range(mat.shape[1]):
                data[f"e{j}"] = mat[:, j]
            frames.append(pl.DataFrame(data))

        out = pl.concat(frames, how="vertical").sort("not_before", "symbol")
        return out.lazy()
