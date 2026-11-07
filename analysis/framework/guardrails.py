"""Automated checks that a strategy plays by the framework's rules.

Look-ahead (truncation invariance): whatever a strategy did up to day t must
be identical whether or not data after t exists. The check runs the strategy
on the full data and on copies truncated at a few cutoff days, then compares
equity and decisions up to each cutoff. It catches look-ahead anywhere --
`shift(-n)` or full-sample statistics in `prepare`, reaching into private
`MarketView` state, non-deterministic decisions -- without reading the code."""

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping

import polars as pl

from analysis.framework.engine import run_backtest
from analysis.framework.market import DATE_COL, MarketView, Panel
from analysis.framework.result import BacktestConfig, BacktestResult
from analysis.framework.strategy import Strategy, build_signals

_CUTOFF_QUANTILES = (0.4, 0.7, 0.9)
_REL_TOLERANCE = 1e-9


@dataclass(frozen=True)
class LookaheadCheck:
    """`passed` is False if any cutoff shows a difference, described in
    `failures`. `result` is the full-data backtest (reusable by the caller,
    so the check costs only the truncated runs)."""

    passed: bool
    cutoffs: list[date]
    failures: list[str] = field(default_factory=list)
    result: BacktestResult | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "cutoffs": [d.isoformat() for d in self.cutoffs],
            "failures": self.failures,
        }


def check_lookahead(
    strategy_cls: type[Strategy],
    params: dict[str, Any],
    data: pl.DataFrame | Panel,
    *,
    config: BacktestConfig = BacktestConfig(),
    risk_free: pl.DataFrame | None = None,
    start: date | None = None,
    quantiles: tuple[float, ...] = _CUTOFF_QUANTILES,
) -> LookaheadCheck:
    """Truncation-invariance check of `strategy_cls(**params)` over the
    evaluated period (from `start`); cutoffs are placed at `quantiles` of its
    trading days."""
    panel = data if isinstance(data, Panel) else Panel(data)
    full = run_backtest(strategy_cls(**params), panel, config, risk_free, start=start)
    evaluated = full.equity[DATE_COL].to_list()
    cutoffs = sorted(
        {evaluated[min(len(evaluated) - 1, int(len(evaluated) * q))] for q in quantiles}
    )
    cutoffs = [c for c in cutoffs if c > evaluated[0]]

    failures: list[str] = []
    for cutoff in cutoffs:
        strategy = strategy_cls(**params)
        truncated_panel = Panel(panel.frame.filter(pl.col(DATE_COL) <= cutoff))
        truncated = run_backtest(
            strategy, truncated_panel, config, risk_free, start=start
        )
        # the engine never asks for a decision on the last day (nothing could
        # execute it), yet that is exactly where a strategy peeking one day
        # ahead differs: ask the same instance for it explicitly
        view = MarketView(
            truncated_panel, cutoff, build_signals(strategy, truncated_panel.frame)
        )
        last = strategy.decide(view, truncated.final_state)
        failures += _compare(full, truncated, last, cutoff)
    return LookaheadCheck(not failures, cutoffs, failures, full)


def _compare(
    full: BacktestResult,
    truncated: BacktestResult,
    last_decision: Mapping[str, float] | None,
    cutoff: date,
) -> list[str]:
    failures = []

    def rows(r: BacktestResult) -> list[tuple]:
        return (
            r.decisions.filter(pl.col(DATE_COL) < cutoff)
            .sort(DATE_COL, "symbol")
            .rows()
        )

    truncated_rows = rows(truncated)
    if last_decision is not None:  # same row format as BacktestResult.decisions
        truncated_rows += [
            (cutoff, s, w) for s, w in sorted(last_decision.items())
        ] or [(cutoff, None, 0.0)]
    full_rows = (
        full.decisions.filter(pl.col(DATE_COL) <= cutoff)
        .sort(DATE_COL, "symbol")
        .rows()
    )

    def differs(a: tuple, b: tuple) -> bool:
        return a[:2] != b[:2] or not _close(a[2], b[2])

    if len(full_rows) != len(truncated_rows) or any(
        differs(a, b) for a, b in zip(full_rows, truncated_rows)
    ):
        mismatched = [a[0] for a, b in zip(full_rows, truncated_rows) if differs(a, b)]
        unmatched = [
            r[0]
            for r in full_rows[len(truncated_rows) :] + truncated_rows[len(full_rows) :]
        ]
        first = min(mismatched + unmatched)
        failures.append(
            f"cutoff {cutoff}: decisions differ when data after the cutoff is removed "
            f"(first difference on {first})"
        )

    full_equity = full.equity.filter(pl.col(DATE_COL) <= cutoff)
    if full_equity[DATE_COL].to_list() != truncated.equity[DATE_COL].to_list() or any(
        not _close(a, b)
        for a, b in zip(
            full_equity["portfolio_value"], truncated.equity["portfolio_value"]
        )
    ):
        failures.append(
            f"cutoff {cutoff}: portfolio value up to the cutoff depends on later data"
        )
    return failures


def _close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(a, b, rel_tol=_REL_TOLERANCE, abs_tol=1e-9)
