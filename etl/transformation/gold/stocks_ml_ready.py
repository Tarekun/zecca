import polars as pl
from typing import Callable

from etl.logger import get_logger
from etl.transformation.gold import StocksDailyGold
from etl.transformation.silver import GoodSymbolsSilver, SymbolEmbeddingsSilver
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT

logger = get_logger(__name__)


Labeling = Callable[[pl.LazyFrame], pl.LazyFrame]


class StocksMlReadyGold(Model):
    """Dataset ready for ML experiments. It filters the symbol universe
    by using those symbols in the model `silver.good_symbols` and joins
    with model `silver.symbol_embeddings` to expose the `embedding`
    column of type list of floats.

    It is of kind="view" and computed on the fly when .build().collect()
    is called"""

    def __init__(
        self,
        labellings: list[Labeling] = [],
        feature_list: list[str] | None = None,
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
    ):
        super().__init__(
            name="stocks_ml_ready",
            layer="gold",
            partitioning_columns=["year", "month"],
            dataplatform_root=dataplatform_root,
            kind="view",
        )
        self.labellings = labellings
        self.feature_list = feature_list

    def _build(self) -> pl.LazyFrame:
        lf = StocksDailyGold(dataplatform_root=self.dataplatform_root).read_from_disk()
        if self.feature_list is not None:
            lf = lf.select("symbol", "timeframe", *self.feature_list)

        good_symbols = GoodSymbolsSilver(
            dataplatform_root=self.dataplatform_root
        ).read_from_disk()
        lf = lf.join(good_symbols, on=["timeframe", "symbol"], how="inner")

        embeddings = SymbolEmbeddingsSilver(
            dataplatform_root=self.dataplatform_root
        ).read_from_disk()
        embedding_cols = [
            col
            for col in embeddings.collect_schema().names()
            if col not in ("symbol", "not_before")
        ]
        embeddings = embeddings.select(
            "symbol",
            "not_before",
            pl.concat_list(embedding_cols).alias("embedding"),
        ).sort(["symbol", "not_before"])
        lf = (
            lf.sort(["symbol", "timeframe"])
            # join_asof requires both frames to be sorted on the by/on columns
            .join_asof(
                embeddings,
                left_on="timeframe",
                right_on="not_before",
                by="symbol",
                strategy="backward",
            ).drop("not_before")
        )

        # TODO: some symbols never appear in any symbol_embeddings partition (they
        # get excluded upstream for insufficient return history), so they can never
        # get a match here. Ideally this shouldn't drop anything -- see
        # tests/data_quality/silver/test_symbol_embeddings.py::test_no_symbol_missing_from_good_symbols
        lf = lf.filter(pl.col("embedding").is_not_null())

        for labelling in self.labellings:
            lf = labelling(lf)

        return lf


def append_future_returns(
    lookahead_steps: int,
    thresholds: list[float] = [],
    price_column: str = "open",
    future_price_col: str = "future_price",
    class_col: str = "price_movement_class",
    custom_labels: list | None = None,
) -> Labeling:
    """Labelling to extend `gold.ml_ready` with the price of each symbol
    `lookahead_steps` timeframes ahead, for price forecasting.

    Appends the column `future_price_col` with the raw price value. If
    `thresholds` is provided, also appends `price_movement_class`, a
    discretization of the percentage price change into classes symmetric
    around zero.

    If `thresholds=[0.01, 0.03]` 5 classes are derived: increase of more than 3%, increase
    between 1%-3%, price stagnation between 1% and -1%, price dicrease of 1-3%, price
    dicrease of more than 3%.

    `custom_labels`, if provided, must have exactly `2 * len(thresholds) + 1`
    elements (one fewer if 0 is included in `thresholds`) and is used to name
    the resulting classes instead of the default integer class IDs.
    """

    def labeler(lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = lf.sort(["symbol", "timeframe"]).with_columns(
            pl.col(price_column)
            .shift(-lookahead_steps)
            .over("symbol")
            .alias(future_price_col)
        )

        if not thresholds:
            return lf

        posi = sorted(thresholds)
        nega = [-t for t in reversed(posi) if t != 0]
        bin_edges = nega + posi
        if custom_labels is not None and len(custom_labels) != len(bin_edges) + 1:
            raise ValueError(
                f"Thresholds {thresholds} produce {len(bin_edges)+1} classes, but only {len(custom_labels)} custom labels were provided"
            )

        future_return = pl.col(future_price_col) / pl.col(price_column) - 1
        return lf.with_columns(
            future_return.cut(bin_edges, labels=custom_labels).alias(class_col)
        )

    return labeler
