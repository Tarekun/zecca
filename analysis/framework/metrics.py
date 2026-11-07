"""Minimal performance metrics used as optimization objectives.

The full metrics/benchmark/report layer (Sortino, Calmar, turnover,
benchmark comparison, Fama-French regression, ...) belongs to the metrics
issue; these are the few numbers walk-forward needs to rank candidates.
Every function takes a `BacktestResult` and returns a float (NaN when
undefined, e.g. no volatility)."""

import math

import polars as pl

from analysis.framework.result import BacktestResult

TRADING_DAYS_PER_YEAR = 252


def daily_returns(result: BacktestResult) -> pl.Series:
    """Close-to-close returns of the portfolio, the first one measured from
    `result.initial_value`."""
    values = pl.concat(
        [pl.Series([result.initial_value]), result.equity["portfolio_value"]]
    )
    return (values / values.shift(1) - 1).slice(1).alias("return")


def sharpe(result: BacktestResult) -> float:
    """Annualized Sharpe ratio of daily returns in excess of the risk-free rate."""
    excess = daily_returns(result) - result.equity["risk_free"]
    std = excess.std()
    if excess.len() < 2 or std is None or std == 0 or not math.isfinite(std):  # type: ignore[arg-type]
        return math.nan
    return float(excess.mean()) / float(std) * math.sqrt(TRADING_DAYS_PER_YEAR)  # type: ignore[arg-type]


def cagr(result: BacktestResult) -> float:
    """Compound annual growth rate, annualized over trading days."""
    days = result.equity.height
    growth = result.final_value / result.initial_value
    if days == 0 or growth <= 0:
        return math.nan
    return growth ** (TRADING_DAYS_PER_YEAR / days) - 1


def volatility(result: BacktestResult) -> float:
    std = daily_returns(result).std()
    return math.nan if std is None else float(std) * math.sqrt(TRADING_DAYS_PER_YEAR)  # type: ignore[arg-type]


def max_drawdown(result: BacktestResult) -> float:
    """Largest peak-to-trough loss as a negative fraction (-0.25 = -25%)."""
    values = pl.concat(
        [pl.Series([result.initial_value]), result.equity["portfolio_value"]]
    )
    return float((values / values.cum_max() - 1).min())  # type: ignore[arg-type]


def summary(result: BacktestResult) -> dict[str, float]:
    return {
        "total_return": result.total_return,
        "cagr": cagr(result),
        "volatility": volatility(result),
        "sharpe": sharpe(result),
        "max_drawdown": max_drawdown(result),
    }
