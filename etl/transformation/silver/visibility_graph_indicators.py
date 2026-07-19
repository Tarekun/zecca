from datetime import date
from pathlib import Path
import networkx as nx
import polars as pl
from dateutil.relativedelta import relativedelta
from ts2vg import NaturalVG

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.good_symbols import GoodSymbolsSilver

FIRST_DATE = date(2020, 1, 1)
FINAL_DATE = date(2026, 1, 1)
TIME_WINDOW_MONTHS = 1
TIME_SHIFT_WEEKS = 1


def build_visibility_graphs(df: pl.DataFrame) -> dict[str, nx.Graph]:
    """Builds a natural visibility graph per symbol found in `df`.

    For each symbol, its 'open' series is taken sorted by 'timeframe' and
    turned into a visibility graph (node i is an edge to node j if the
    corresponding price points can "see" each other, per the natural
    visibility graph criterion)."""
    graphs = {}
    for (symbol,), group in df.group_by("symbol", maintain_order=True):
        prices = group.sort("timeframe")["open"].to_numpy(writable=True)
        vg = NaturalVG()
        vg.build(prices)
        graphs[symbol] = vg.as_networkx()
    return graphs


def compute_graph_metrics(graph: nx.Graph) -> dict[str, float]:
    """Computes summary metrics of a visibility graph. For now, only the
    average shortest path length (average number of hops needed to get from
    any node to any other node)."""
    return {
        "avg_shortest_path_length": nx.average_shortest_path_length(graph),
    }


def compute_visibility_graph_indicators(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rolls a `TIME_WINDOW_MONTHS`-wide window over the series (moving it
    forward by `TIME_SHIFT_WEEKS` each step), and for each window builds the
    per-symbol visibility graph and its metrics. Each output row's `timeframe`
    is the last (closing) timeframe covered by the window it was computed on."""
    df = (
        lf.select("timeframe", "symbol", "open")
        .filter(
            (pl.col("timeframe") > FIRST_DATE) & (pl.col("timeframe") <= FINAL_DATE)
        )
        .collect()
    )

    rows = []
    window_end = FIRST_DATE + relativedelta(months=TIME_WINDOW_MONTHS)
    while window_end <= FINAL_DATE:
        window_start = window_end - relativedelta(months=TIME_WINDOW_MONTHS)
        window_df = df.filter(
            (pl.col("timeframe") > window_start) & (pl.col("timeframe") <= window_end)
        )
        for symbol, graph in build_visibility_graphs(window_df).items():
            if graph.number_of_nodes() < 2:
                continue
            metrics = compute_graph_metrics(graph)
            rows.append({"timeframe": window_end, "symbol": symbol, **metrics})

        window_end = window_end + relativedelta(weeks=TIME_SHIFT_WEEKS)

    schema = {
        "timeframe": df.schema["timeframe"],
        "symbol": df.schema["symbol"],
        "avg_shortest_path_length": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema).lazy()


class VisibilityGraphIndicatorsSilver(Model):
    def __init__(
        self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT
    ) -> None:
        super().__init__(
            name="visibility_graph_indicators",
            layer="silver",
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        candles = CandlesDailySilver(
            yfinance_data_path="", dataplatform_root=self.dataplatform_root
        )
        good_symbols = GoodSymbolsSilver(dataplatform_root=self.dataplatform_root)
        lf = (
            candles.read_from_disk()
            .select("timeframe", "symbol", "open")
            .join(
                good_symbols.read_from_disk(),
                on=["timeframe", "symbol"],
                how="inner",
            )
        )
        return compute_visibility_graph_indicators(lf)
