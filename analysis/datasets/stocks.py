import re
from datetime import date

import numpy as np
import polars as pl
import torch
from torch.utils.data import Dataset


from etl.transformation.gold import StocksDailyGold
from etl.transformation.silver import SymbolEmbeddingsSilver
from etl.transformation.model import DEFAULT_DATAPLATFORM_ROOT

_EMBEDDING_COL = re.compile(r"^e\d+$")


def load_dataset(
    feature_list: list[str],
    start_date: date,
    end_date: date,
    dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
) -> pl.LazyFrame:
    stocks = (
        StocksDailyGold(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .filter(pl.col("timeframe") >= start_date, pl.col("timeframe") <= end_date)
        .select("symbol", "timeframe", *feature_list)
        .sort("timeframe")
    )
    symbol_embeddings = (
        SymbolEmbeddingsSilver(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .sort("not_before")
    )

    return (
        stocks.join_asof(
            symbol_embeddings,
            left_on="timeframe",
            right_on="not_before",
            by="symbol",
            strategy="backward",
        )
        # rows earlier than the first available embedding for their symbol
        # don't match anything in the asof join and stay null -- drop them
        .filter(pl.col("not_before").is_not_null())
        .drop("not_before")
    )


def append_future_returns(
    df: pl.LazyFrame,
    lookahead_steps: int,
    thresholds: list[float] = [],
) -> pl.LazyFrame:
    """Extends `df` (the output of `load_dataset`, which must include `open`
    in its `feature_list`) with the price of each symbol `lookahead_steps`
    timeframes ahead, for price forecasting.

    Appends the column `future_price` with the raw price value. If
    `thresholds` is provided, also appends `price_movement_class`, a
    discretization of the percentage price change into classes symmetric
    around zero.

    If `thresholds=[0.01, 0.03]` 5 classes are derived: increase of more than 3%, increase
    between 1%-3%, price stagnation between 1% and -1%, price dicrease of 1-3%, price
    dicrease of more than 3%.
    """
    df = df.sort(["symbol", "timeframe"]).with_columns(
        pl.col("open").shift(-lookahead_steps).over("symbol").alias("future_price")
    )

    if not thresholds:
        return df

    posi = sorted(thresholds)
    nega = [-t for t in reversed(posi) if t != 0]
    bin_edges = nega + posi
    future_return = pl.col("future_price") / pl.col("open") - 1
    return df.with_columns(future_return.cut(bin_edges).alias("price_movement_class"))


def make_tensor_series(
    df: pl.LazyFrame, series_length: int
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """`df` must be the output of `append_future_returns` with `thresholds` set,
    i.e. it must carry a `future_price` and a `price_movement_class` column.
    `future_price` (the raw price) is dropped -- it was only an intermediate
    for deriving `price_movement_class`, the label this function returns."""

    embedding_cols = sorted(
        (c for c in df.columns if _EMBEDDING_COL.match(c)), key=lambda c: int(c[1:])
    )
    df = df.drop("future_price")
    feature_cols = [
        c
        for c in df.columns
        if c not in ("symbol", "timeframe", "price_movement_class", *embedding_cols)
    ]

    df = df.sort(["symbol", "timeframe"])

    # lag[i] is each feature's value `i` rows back within its symbol, so lag 0
    # is the current (most recent) reading and lag `series_length - 1` the
    # oldest -- i.e. decreasing timeframe, the order the CNN expects
    lag_cols = [
        pl.col(feat).shift(i).over("symbol").alias(f"{feat}__{i}")
        for feat in feature_cols
        for i in range(series_length)
    ]
    lag_names = [f"{feat}__{i}" for feat in feature_cols for i in range(series_length)]
    # the only point in this pipeline where the LazyFrame gets materialized
    window = (
        df.select(
            "symbol", "timeframe", "price_movement_class", *embedding_cols, *lag_cols
        )
        .filter(
            pl.all_horizontal(
                pl.col(lag_names).is_not_null(),
                pl.col(lag_names).is_finite(),
                pl.col("price_movement_class").is_not_null(),
            )
        )
        .collect()
    )

    series = np.stack(
        [
            window.select([f"{feat}__{i}" for i in range(series_length)]).to_numpy()
            for feat in feature_cols
        ],
        axis=1,
    )  # (N, C, L)
    embedding = window.select(embedding_cols).to_numpy()  # (N, E)
    labels = window["price_movement_class"].to_physical().to_numpy()  # (N,)

    return (
        torch.as_tensor(series, dtype=torch.float32),
        torch.as_tensor(embedding, dtype=torch.float32),
        torch.as_tensor(labels, dtype=torch.long),
    )


def compute_normalization_stats(
    series: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Per-feature-channel mean/std of a `series` tensor (shape `(N, C, L)`,
    as returned by `make_tensor_series`), for z-score normalizing it. Fit this
    on the training set only, then reuse the same stats to normalize
    validation/test sets via `normalize_series` -- fitting separately per
    split would leak each split's own scale into itself, and isn't how the
    model sees data at inference time anyway."""
    mean = series.mean(dim=(0, 2), keepdim=True)
    std = series.std(dim=(0, 2), keepdim=True).clamp_min(1e-8)
    return mean, std


def normalize_series(
    series: torch.Tensor, mean: torch.Tensor, std: torch.Tensor
) -> torch.Tensor:
    return (series - mean) / std


class StocksDataset(Dataset):
    def __init__(
        self,
        feature_list: list[str],
        start_date: date,
        end_date: date,
        series_length: int,
        lookahead_steps: int,
        thresholds: list[float],
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
        normalization_stats: tuple[torch.Tensor, torch.Tensor] | None = None,
    ) -> None:
        """`normalization_stats`, if given, is a `(mean, std)` pair from a
        prior call to `compute_normalization_stats` -- pass the training
        set's own `normalization_stats` attribute here when building the
        validation/test sets. If omitted (as for the training set itself),
        stats are fit on this dataset's own `series`."""
        df = load_dataset(feature_list, start_date, end_date, dataplatform_root)
        df = append_future_returns(df, lookahead_steps, thresholds)
        self.series, self.embedding, self.labels = make_tensor_series(
            df, series_length
        )
        self.normalization_stats = normalization_stats or compute_normalization_stats(
            self.series
        )
        self.series = normalize_series(self.series, *self.normalization_stats)

    def __len__(self) -> int:
        return self.series.shape[0]

    def __getitem__(
        self, idx: int
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return self.series[idx], self.embedding[idx], self.labels[idx]
