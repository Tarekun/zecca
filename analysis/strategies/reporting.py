import polars as pl
import matplotlib.pyplot as plt

_TRADING_DAYS_PER_YEAR = 252


def _compute_metrics(history: pl.DataFrame) -> dict[str, float | None]:
    """Annualized volatility/Sharpe (rf=0) plus max drawdown and its longest
    duration (days underwater since the last equity peak) from the equity curve."""
    enriched = (
        history.with_columns(
            pl.col("portfolio_value").pct_change().alias("daily_return"),
            pl.col("portfolio_value").cum_max().alias("running_max"),
        )
        .with_columns(
            (pl.col("portfolio_value") / pl.col("running_max") - 1).alias("drawdown"),
            pl.when(pl.col("portfolio_value") >= pl.col("running_max"))
            .then(pl.col("timeframe"))
            .otherwise(None)
            .forward_fill()
            .alias("last_peak_date"),
        )
        .with_columns(
            (pl.col("timeframe") - pl.col("last_peak_date"))
            .dt.total_days()
            .alias("drawdown_days")
        )
    )

    daily_return_mean = enriched["daily_return"].mean()
    daily_return_std = enriched["daily_return"].std()
    max_drawdown = enriched["drawdown"].min()
    longest_drawdown_days = enriched["drawdown_days"].max()

    return {
        "volatility": (
            float(daily_return_std) * _TRADING_DAYS_PER_YEAR**0.5  # type: ignore
            if daily_return_std is not None
            else None
        ),
        "sharpe": (
            float(daily_return_mean) / float(daily_return_std) * _TRADING_DAYS_PER_YEAR**0.5  # type: ignore
            if daily_return_mean is not None and daily_return_std
            else None
        ),
        "max_drawdown": float(max_drawdown) if max_drawdown is not None else None,  # type: ignore
        "longest_drawdown_days": (
            float(longest_drawdown_days) if longest_drawdown_days is not None else None  # type: ignore
        ),
    }


def _fmt_pct(value: float | None) -> str:
    return f"{value:.2%}" if value is not None else "N/A"


def _fmt_num(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "N/A"


def plot_result(history: pl.DataFrame, title: str | None = None):
    # show a pyplot of the equity curve from simulate_strategy, annotated with
    # volatility, longest drawdown period, maximum drawdown value and overall sharpe
    metrics = _compute_metrics(history)

    plt.figure(figsize=(12, 6))
    plt.plot(history["timeframe"], history["portfolio_value"], label="Portfolio value")
    plt.xlabel("Date")
    plt.ylabel("Portfolio value")
    plt.title(
        f"Classic momentum strategy equity curve — {title}"
        if title
        else "Classic momentum strategy equity curve"
    )
    plt.legend(loc="upper left")
    plt.grid(True)

    stats_text = (
        f"Volatility (ann.): {_fmt_pct(metrics['volatility'])}\n"
        f"Sharpe (ann.): {_fmt_num(metrics['sharpe'])}\n"
        f"Max drawdown: {_fmt_pct(metrics['max_drawdown'])}\n"
        f"Longest drawdown: {_fmt_num(metrics['longest_drawdown_days'])} days"
    )
    plt.gca().text(
        0.02,
        0.02,
        stats_text,
        transform=plt.gca().transAxes,
        fontsize=10,
        verticalalignment="bottom",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
    )

    plt.show()
