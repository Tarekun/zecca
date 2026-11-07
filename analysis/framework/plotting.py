"""Quick-look charts for backtest results (kept out of the package's
top-level imports so the framework doesn't require matplotlib)."""

from datetime import date
from typing import Iterable

import matplotlib.pyplot as plt

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
