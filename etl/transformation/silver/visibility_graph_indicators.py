from datetime import date
from pathlib import Path
import networkx as nx
import numpy as np
import polars as pl
from dateutil.relativedelta import relativedelta
from ts2vg import NaturalVG

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.good_symbols import GoodSymbolsSilver

FIRST_DATE = date(2000, 1, 1)
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
    """Computes summary metrics of a visibility graph, used as technical
    indicators of the price series over the window the graph was built on.

    Visibility graphs turn structural properties of a time series (trend
    persistence, volatility clustering, extreme events...) into
    graph-theoretic ones, so each metric below is a proxy for some dynamical
    property of the underlying price series. For further reading:
      - Lacasa, L., Luque, B., Ballesteros, F., Luque, J., & Nuno, J. C.
        (2008). From time series to complex networks: The visibility graph.
        PNAS, 105(13), 4972-4975.
      - Luque, B., Lacasa, L., Ballesteros, F., & Luque, J. (2009).
        Horizontal visibility graphs: Exact results for random time series.
        Physical Review E, 80(4), 046103.
      - Yang, Y., Wang, J., Yang, H., & Mang, J. (2009). Visibility graph
        approach to exchange rate series. Physica A, 388(20), 4431-4437.
      - Clauset, A., Shalizi, C. R., & Newman, M. E. J. (2009). Power-law
        distributions in empirical data. SIAM Review, 51(4), 661-703.
    """
    degrees = np.array([d for _, d in graph.degree()])
    betweenness = nx.betweenness_centrality(graph)

    # degree distribution, used below to fit its power-law exponent. The fit
    # needs at least 2 distinct degree values to define a line; windows too
    # small or too uniform for that yield an undefined (NaN) exponent.
    values, counts = np.unique(degrees, return_counts=True)
    mask = values > 0
    log_k = np.log(values[mask])
    log_p = np.log(counts[mask] / counts.sum())
    if len(log_k) >= 2:
        slope, _ = np.polyfit(log_k, log_p, 1)
    else:
        slope = float("nan")

    return {
        # average number of hops needed to get from any node to any other
        # node: shorter paths mean price points stay "mutually visible"
        # across the whole window, i.e. a smoother, more persistent trend.
        "avg_shortest_path_length": nx.average_shortest_path_length(graph),
        # mean number of edges per node: high-degree nodes are price points
        # visible from many others, so this tracks how much the window is
        # dominated by a handful of standout highs/lows.
        "average_degree": float(np.mean(degrees)),
        # fraction of a node's neighbors that are themselves connected,
        # averaged over all nodes: measures local "cliquishness", which in
        # visibility graphs correlates with volatility clustering (calm
        # stretches form tight local cliques, turbulent ones don't).
        "average_clustering_coefficient": nx.average_clustering(graph),
        # Pearson correlation between degrees of connected nodes: positive
        # values mean extreme price points tend to connect to other extreme
        # ones (hierarchical structure), negative values mean they connect
        # to "ordinary" points instead. Useful for spotting regime changes.
        "degree_assortativity_coefficient": nx.degree_assortativity_coefficient(graph),
        # ratio of actual to possible edges: a cheap, coarse measure of how
        # "peaky" (sparse, few dominant points) vs. "smooth" (dense, most
        # points mutually visible) the window's price action was.
        "graph_density": nx.density(graph),
        # mean fraction of shortest paths passing through each node: high
        # average betweenness means the window is bridged by a small number
        # of pivotal points, typical of sharp spikes/crashes rather than
        # gradual moves.
        "average_betweenness_centrality": float(np.mean(list(betweenness.values()))),
        # exponent of a power law fit to the degree distribution via a
        # log-log linear regression (see Clauset et al. 2009 for a more
        # rigorous MLE-based alternative). Visibility graphs of fractal,
        # long-range-correlated series (e.g. persistent trends) show a
        # power-law degree distribution, while uncorrelated random-walk-like
        # series produce an exponential one: analogous in spirit to the
        # Hurst exponent.
        "degree_distribution_power_law_exponent": float(-slope),
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
        "average_degree": pl.Float64,
        "average_clustering_coefficient": pl.Float64,
        "degree_assortativity_coefficient": pl.Float64,
        "graph_density": pl.Float64,
        "average_betweenness_centrality": pl.Float64,
        "degree_distribution_power_law_exponent": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema).lazy()


class VisibilityGraphIndicatorsSilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
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
