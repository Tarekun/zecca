import logging
import math
from bisect import bisect_left
from datetime import date, timedelta
from typing import Iterable, Mapping

import polars as pl

from analysis.framework.costs import CostModel
from analysis.framework.market import DATE_COL, SYMBOL_COL, MarketView, Panel
from analysis.framework.portfolio import PortfolioState
from analysis.framework.result import BacktestConfig, BacktestResult
from analysis.framework.strategy import Strategy, build_signals
from analysis.framework.weights import TargetWeights

logger = logging.getLogger(__name__)

_TRADING_DAYS_PER_YEAR = 252
_STALE_RISK_FREE = timedelta(days=31)
# quantities below this are float noise left over from sells, not positions
_QTY_EPSILON = 1e-12

_TRADES_SCHEMA = {
    DATE_COL: pl.Date,
    SYMBOL_COL: pl.String,
    "side": pl.String,
    "quantity": pl.Float64,
    "quote": pl.Float64,
    "price": pl.Float64,
    "notional": pl.Float64,
    "commission": pl.Float64,
    "slippage": pl.Float64,
    "reason": pl.String,
}
_POSITIONS_SCHEMA = {
    DATE_COL: pl.Date,
    SYMBOL_COL: pl.String,
    "quantity": pl.Float64,
    "price": pl.Float64,
    "value": pl.Float64,
    "weight": pl.Float64,
    "stale_days": pl.Int64,
}
_EQUITY_SCHEMA = {
    DATE_COL: pl.Date,
    "portfolio_value": pl.Float64,
    "cash": pl.Float64,
    "invested": pl.Float64,
    "exposure": pl.Float64,
    "interest": pl.Float64,
    "risk_free": pl.Float64,
}
_DECISIONS_SCHEMA = {DATE_COL: pl.Date, SYMBOL_COL: pl.String, "weight": pl.Float64}
_EVENTS_SCHEMA = {
    DATE_COL: pl.Date,
    SYMBOL_COL: pl.String,
    "kind": pl.String,
    "detail": pl.String,
}


def run_backtest(
    strategy: Strategy,
    data: pl.DataFrame | pl.LazyFrame | Panel,
    config: BacktestConfig = BacktestConfig(),
    risk_free: pl.DataFrame | None = None,
    start: date | None = None,
    initial: PortfolioState | None = None,
) -> BacktestResult:
    """Simulates `strategy` day by day over `data`.

    `data` is the market panel: one row per (`timeframe`, `symbol`) with at
    least `open` and `close` (prices adjusted for splits and dividends), plus
    any other column the strategy reads. `risk_free` is an optional
    `(timeframe, rf)` frame of daily decimal returns (see
    `data.load_risk_free`); without it cash earns and borrowing costs nothing
    but the spread.

    `start` makes everything before it warmup history: visible to `prepare`
    and `MarketView.history`, but not traded. The strategy is asked to
    decide at the close of the last trading day before `start`, so trading
    can begin at the open of the first trading day >= `start`. `initial`
    starts the simulation from an existing portfolio instead of
    `config.initial_cash` (used to chain walk-forward folds).

    Timeline of every trading day t:
    1. open: the target weights decided at the close of t-1 are executed at
       the open price, net of `config.costs`; exits that couldn't execute
       earlier are retried;
    2. close: positions are marked at the close (or at their last known price
       when today's is missing), positions without a price for
       `config.delisting_days` are force-liquidated, interest accrues on cash;
    3. decision: `strategy.decide` sees data up to t's close and may return
       new target weights, executed at the open of t+1 (a decision on the
       last day is never executed, so it isn't requested).
    """
    if isinstance(data, Panel):
        market = data
    else:
        market = Panel(data.collect() if isinstance(data, pl.LazyFrame) else data)
    missing = [c for c in ("open", "close") if c not in market.columns]
    if missing:
        raise ValueError(f"Market data is missing required column(s) {missing}")
    if not market.dates:
        raise ValueError("Market data is empty")

    first = 0 if start is None else bisect_left(market.dates, start)
    if first == len(market.dates):
        raise ValueError(f"No trading days on or after start {start}")

    signals = build_signals(strategy, market.frame)
    rates = _align_risk_free(risk_free, market.dates)
    sim = _Simulation(config, initial)

    def decide(day: date) -> None:
        view = MarketView(market, day, signals)
        weights = strategy.decide(view, sim.portfolio_state(day))
        if weights is not None:
            sim.set_target(day, _as_target(weights), view.universe())

    if first > 0:
        decide(market.dates[first - 1])
    last = len(market.dates) - 1
    for i in range(first, last + 1):
        day = market.dates[i]
        today = market.between(day, day)
        sim.open(day, today)
        sim.close(day, today, rates[i])
        if i < last:
            decide(day)

    return BacktestResult(
        strategy=strategy.name,
        params=dict(strategy.params),
        config=config,
        initial_value=sim.initial_value,
        final_state=sim.portfolio_state(market.dates[last]),
        **sim.tables(),
    )


def _as_target(weights: object) -> TargetWeights:
    if isinstance(weights, TargetWeights):
        return weights
    if isinstance(weights, Mapping):
        return TargetWeights(weights)
    raise TypeError(
        f"decide() must return TargetWeights or None, got {type(weights).__name__}"
    )


def _align_risk_free(risk_free: pl.DataFrame | None, dates: list[date]) -> list[float]:
    """Daily risk-free return for every trading day: the most recent rate
    published on or before that day (rates lag behind prices)."""
    if risk_free is None:
        return [0.0] * len(dates)
    missing = [c for c in (DATE_COL, "rf") if c not in risk_free.columns]
    if missing:
        raise ValueError(f"risk_free is missing column(s) {missing}")

    rf = (
        risk_free.select(pl.col(DATE_COL).cast(pl.Date), pl.col("rf").cast(pl.Float64))
        .drop_nulls()
        .sort(DATE_COL)
    )
    if rf.is_empty():
        raise ValueError("risk_free has no rates")
    if rf[DATE_COL][0] > dates[0]:
        logger.warning(
            "Risk-free rate starts on %s, after the first trading day %s: earlier days use 0",
            rf[DATE_COL][0],
            dates[0],
        )
    if dates[-1] - rf[DATE_COL][-1] > _STALE_RISK_FREE:
        logger.warning(
            "Risk-free rate ends on %s: its last value is reused until %s",
            rf[DATE_COL][-1],
            dates[-1],
        )
    aligned = pl.DataFrame({DATE_COL: dates}, schema={DATE_COL: pl.Date}).join_asof(
        rf, on=DATE_COL, strategy="backward"
    )
    return aligned["rf"].fill_null(0.0).to_list()


def _quotes(
    today: pl.DataFrame, symbols: Iterable[str], column: str
) -> dict[str, float]:
    """Usable `column` prices of `symbols` today (null, NaN and non-positive
    prices count as missing)."""
    symbols = list(symbols)
    if not symbols:
        return {}
    rows = today.filter(
        pl.col(SYMBOL_COL).is_in(symbols)
        & pl.col(column).is_not_null()
        & pl.col(column).is_finite()
        & (pl.col(column) > 0)
    )
    return dict(zip(rows[SYMBOL_COL].to_list(), rows[column].to_list()))


class _Simulation:
    """Mutable portfolio state plus the records that become `BacktestResult`."""

    def __init__(self, config: BacktestConfig, initial: PortfolioState | None) -> None:
        self.config = config
        self.costs: CostModel = config.costs
        if initial is None:
            initial = PortfolioState.empty(date.min, config.initial_cash)
        self.initial_value = initial.net_worth
        self.cash = initial.cash
        self.positions: dict[str, float] = dict(initial.positions)
        self.last_price: dict[str, float] = dict(initial.prices)
        self.stale_days: dict[str, int] = {s: 0 for s in self.positions}
        self.pending_target: TargetWeights | None = None
        self.pending_exits: set[str] = set()
        self._priced_today: set[str] = set()

        self.equity: list[tuple] = []
        self.trades: list[tuple] = []
        self.holdings: list[tuple] = []
        self.decisions: list[tuple] = []
        self.events: list[tuple] = []

    # ---- phases of a trading day --------------------------------------

    def open(self, day: date, today: pl.DataFrame) -> None:
        target, self.pending_target = self.pending_target, None
        symbols = set(self.positions) | set(target or ()) | self.pending_exits
        quotes = _quotes(today, symbols, "open")
        self._priced_today = set(quotes)
        for symbol, quote in quotes.items():
            if symbol in self.positions:
                self._mark(symbol, quote)

        if target is not None:
            # a new decision supersedes exits still waiting from older ones
            self.pending_exits.clear()
            self._rebalance(day, target, quotes)
        elif self.pending_exits:
            self._retry_exits(day, quotes)

    def close(self, day: date, today: pl.DataFrame, rate: float) -> None:
        for symbol, price in _quotes(today, self.positions, "close").items():
            self._mark(symbol, price)
            self._priced_today.add(symbol)

        for symbol in list(self.positions):
            if symbol in self._priced_today:
                continue
            self.stale_days[symbol] += 1
            if self.stale_days[symbol] >= self.config.delisting_days:
                self._force_liquidate(day, symbol)

        interest = self._accrue_interest(rate)
        invested = self._invested()
        value = self.cash + invested
        exposure = invested / value if value > 0 else math.nan
        self.equity.append((day, value, self.cash, invested, exposure, interest, rate))
        for symbol, qty in sorted(self.positions.items()):
            price = self.last_price[symbol]
            weight = qty * price / value if value > 0 else math.nan
            self.holdings.append(
                (day, symbol, qty, price, qty * price, weight, self.stale_days[symbol])
            )

    def set_target(self, day: date, target: TargetWeights, universe: list[str]) -> None:
        target.validate(max_leverage=self.config.max_leverage, universe=universe)
        self.pending_target = target
        if not target:
            self.decisions.append((day, None, 0.0))
        for symbol, weight in sorted(target.items()):
            self.decisions.append((day, symbol, weight))

    def portfolio_state(self, day: date) -> PortfolioState:
        return PortfolioState(
            date=day,
            cash=self.cash,
            positions=self.positions,
            prices={s: self.last_price[s] for s in self.positions},
        )

    # ---- trading ------------------------------------------------------

    def _rebalance(
        self, day: date, target: TargetWeights, quotes: dict[str, float]
    ) -> None:
        net_worth = self.cash + self._invested()
        if net_worth <= 0:
            self._event(
                day,
                None,
                "rebalance_skipped",
                f"non-positive net worth {net_worth:.2f}",
            )
            return

        sells: list[tuple[str, float, float, str]] = []
        buys: list[tuple[str, float, float]] = []
        for symbol in sorted(set(self.positions) | set(target)):
            weight = target.get(symbol, 0.0)
            held = self.positions.get(symbol, 0.0)
            if weight == 0 and held == 0:
                continue
            if symbol not in quotes:
                if weight == 0:
                    self.pending_exits.add(symbol)
                    self._event(
                        day, symbol, "exit_deferred", "no open price, retried daily"
                    )
                elif held == 0:
                    self._event(
                        day, symbol, "buy_skipped", "no open price, weight left in cash"
                    )
                else:
                    self._event(
                        day,
                        symbol,
                        "rebalance_skipped",
                        "no open price, position unchanged",
                    )
                continue

            quote = quotes[symbol]
            if weight == 0:
                sells.append((symbol, held, quote, "rebalance"))
                continue
            target_qty = weight * net_worth / quote
            if not self.config.fractional_shares:
                target_qty = math.floor(target_qty)
            delta = target_qty - held
            if delta == 0 or abs(delta) * quote < self.config.min_trade_value:
                continue
            if delta < 0:
                sells.append((symbol, -delta, quote, "rebalance"))
            else:
                buys.append((symbol, delta, quote))

        for symbol, qty, quote, reason in sells:
            self._sell(day, symbol, qty, quote, reason)
        self._buy_within_budget(day, buys)

    def _buy_within_budget(
        self, day: date, buys: list[tuple[str, float, float]]
    ) -> None:
        """Executes `buys`, scaling them down proportionally if, together,
        they would push exposure past `max_leverage` (cash below 0 when
        unlevered) once costs are included."""
        if not buys:
            return
        invested = self._invested()
        budget = self.config.max_leverage * (self.cash + invested) - invested

        def spend(qty: float, quote: float) -> float:
            notional = qty * self.costs.execution_price(quote, "buy")
            return notional + self.costs.commission(notional)

        total = math.fsum(spend(qty, quote) for _, qty, quote in buys)
        scale = 1.0 if total <= budget else max(budget, 0.0) / total
        if scale < 1.0:
            self._event(
                day,
                None,
                "buys_scaled",
                f"scaled to {scale:.6f} to respect max leverage",
            )
        for symbol, qty, quote in buys:
            qty *= scale
            if not self.config.fractional_shares:
                qty = math.floor(qty)
            if qty > 0:
                self._buy(day, symbol, qty, quote)

    def _retry_exits(self, day: date, quotes: dict[str, float]) -> None:
        for symbol in sorted(self.pending_exits):
            if symbol not in self.positions:
                self.pending_exits.discard(symbol)
            elif symbol in quotes:
                self._sell(
                    day, symbol, self.positions[symbol], quotes[symbol], "exit_retry"
                )
                self.pending_exits.discard(symbol)

    def _buy(self, day: date, symbol: str, qty: float, quote: float) -> None:
        price = self.costs.execution_price(quote, "buy")
        notional = qty * price
        commission = self.costs.commission(notional)
        self.cash -= notional + commission
        self.positions[symbol] = self.positions.get(symbol, 0.0) + qty
        self.stale_days.setdefault(symbol, 0)
        self._mark(symbol, quote)
        self.trades.append(
            (
                day,
                symbol,
                "buy",
                qty,
                quote,
                price,
                notional,
                commission,
                qty * (price - quote),
                "rebalance",
            )
        )

    def _sell(
        self, day: date, symbol: str, qty: float, quote: float, reason: str
    ) -> None:
        qty = min(qty, self.positions[symbol])
        price = self.costs.execution_price(quote, "sell")
        notional = qty * price
        commission = self.costs.commission(notional)
        self.cash += notional - commission
        self._reduce(symbol, qty)
        self.trades.append(
            (
                day,
                symbol,
                "sell",
                qty,
                quote,
                price,
                notional,
                commission,
                qty * (quote - price),
                reason,
            )
        )

    def _force_liquidate(self, day: date, symbol: str) -> None:
        qty = self.positions[symbol]
        quote = self.last_price[symbol]
        price = quote * (1 - self.config.delisting_haircut)
        self.cash += qty * price
        self._reduce(symbol, qty)
        self.pending_exits.discard(symbol)
        self.trades.append(
            (day, symbol, "sell", qty, quote, price, qty * price, 0.0, 0.0, "delisting")
        )
        self._event(
            day,
            symbol,
            "delisting",
            f"no price for {self.config.delisting_days} trading days, liquidated at "
            f"{price:.4f} (last price {quote:.4f}, haircut {self.config.delisting_haircut:.0%})",
        )

    # ---- bookkeeping --------------------------------------------------

    def _mark(self, symbol: str, price: float) -> None:
        self.last_price[symbol] = price
        self.stale_days[symbol] = 0

    def _reduce(self, symbol: str, qty: float) -> None:
        remaining = self.positions[symbol] - qty
        if remaining <= _QTY_EPSILON * max(1.0, qty):
            del self.positions[symbol]
            del self.stale_days[symbol]
        else:
            self.positions[symbol] = remaining

    def _invested(self) -> float:
        return math.fsum(qty * self.last_price[s] for s, qty in self.positions.items())

    def _accrue_interest(self, rate: float) -> float:
        if self.cash > 0:
            interest = self.cash * rate if self.config.cash_earns_interest else 0.0
        else:
            interest = self.cash * (
                rate + self.config.borrow_spread / _TRADING_DAYS_PER_YEAR
            )
        self.cash += interest
        return interest

    def _event(self, day: date, symbol: str | None, kind: str, detail: str) -> None:
        self.events.append((day, symbol, kind, detail))

    def tables(self) -> dict[str, pl.DataFrame]:
        def frame(rows: list[tuple], schema: dict) -> pl.DataFrame:
            return pl.DataFrame(rows, schema=schema, orient="row")

        return {
            "equity": frame(self.equity, _EQUITY_SCHEMA),
            "trades": frame(self.trades, _TRADES_SCHEMA),
            "positions": frame(self.holdings, _POSITIONS_SCHEMA),
            "decisions": frame(self.decisions, _DECISIONS_SCHEMA),
            "events": frame(self.events, _EVENTS_SCHEMA),
        }
