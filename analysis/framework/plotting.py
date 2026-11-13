"""Quick-look charts for backtest results (kept out of the package's
top-level imports so the framework doesn't require matplotlib)."""

from datetime import date
from typing import Any, Iterable

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np

from analysis.framework.metrics import summary
from analysis.framework.result import BacktestResult


def plot_result(
    result: BacktestResult,
    title: str | None = None,
    fold_starts: Iterable[date] = (),
) -> None:
    """Equity curve (top) and drawdown (bottom) annotated with the summary
    metrics. For a walk-forward run pass `result.oos` and
    `fold_starts=wf.folds["test_start"]` to mark where parameters changed."""
    equity = result.equity
    values = equity["portfolio_value"]
    drawdown = values / values.cum_max() - 1
    metrics = summary(result)

    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(12, 7), sharex=True, gridspec_kw={"height_ratios": [3, 1]}
    )
    top.plot(equity["timeframe"], values, label="Portfolio value")
    top.set_ylabel("Portfolio value")
    top.set_title(title or f"{result.strategy} {dict(result.params) or ''}".strip())
    top.grid(True)
    bottom.fill_between(equity["timeframe"], drawdown, 0, color="tab:red", alpha=0.4)
    bottom.set_ylabel("Drawdown")
    bottom.grid(True)
    for start in fold_starts:
        for ax in (top, bottom):
            ax.axvline(start, color="grey", linewidth=0.6, linestyle="--")

    top.text(
        0.02,
        0.97,
        "\n".join(
            [
                f"Total return: {metrics['total_return']:.1%}",
                f"CAGR: {metrics['cagr']:.1%}",
                f"Volatility (ann.): {metrics['volatility']:.1%}",
                f"Sharpe (ann., excess): {metrics['sharpe']:.2f}",
                f"Max drawdown: {metrics['max_drawdown']:.1%}",
            ]
        ),
        transform=top.transAxes,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
    )
    fig.tight_layout()
    plt.show()


def plot_sensitivity(sensitivity: dict[str, Any], x: str, y: str) -> None:
    """Heatmap of the mean score over two parameters, from the output of
    `robustness.parameter_sensitivity` (averaged over the other parameters).
    The cell of the best candidate is outlined."""
    scores = sensitivity["scores"]
    xs = sorted({r["params"][x] for r in scores}, key=_sort_key)
    ys = sorted({r["params"][y] for r in scores}, key=_sort_key)
    grid = np.full((len(ys), len(xs)), np.nan)
    for i, yv in enumerate(ys):
        for j, xv in enumerate(xs):
            cell = [
                r["score"]
                for r in scores
                if r["params"][x] == xv
                and r["params"][y] == yv
                and isinstance(r["score"], (int, float))
                and np.isfinite(r["score"])
            ]
            grid[i, j] = np.mean(cell) if cell else np.nan

    fig, ax = plt.subplots(figsize=(1.2 * len(xs) + 3, 0.8 * len(ys) + 2))
    image = ax.imshow(grid, cmap="RdYlGn", origin="lower", aspect="auto")
    ax.set_xticks(range(len(xs)), [str(v) for v in xs])
    ax.set_yticks(range(len(ys)), [str(v) for v in ys])
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    for i in range(len(ys)):
        for j in range(len(xs)):
            if np.isfinite(grid[i, j]):
                ax.text(j, i, f"{grid[i, j]:.2f}", ha="center", va="center", fontsize=9)
    best = sensitivity["best_params"]
    ax.add_patch(
        Rectangle(
            (xs.index(best[x]) - 0.5, ys.index(best[y]) - 0.5),
            1,
            1,
            fill=False,
            edgecolor="black",
            linewidth=3,
        )
    )
    ax.set_title(
        f"Mean score by {x} and {y}"
        + (" (isolated optimum!)" if sensitivity.get("isolated") else "")
    )
    fig.colorbar(image, ax=ax)
    fig.tight_layout()
    plt.show()


def _sort_key(value: Any) -> tuple:
    return (0, value, "") if isinstance(value, (int, float)) else (1, 0, str(value))
