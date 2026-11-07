from bisect import bisect_left, bisect_right
from datetime import date
from typing import Sequence

import polars as pl

from analysis.framework.periods import Period, period_key

DATE_COL = "timeframe"
SYMBOL_COL = "symbol"
# optional boolean column of the market data: is the symbol investable that
# day (e.g. an index member as of that date)? Without it, every symbol with
# data on a day is investable on that day.
UNIVERSE_COL = "in_universe"


class Panel:
    """An immutable (date, symbol) panel -- one row per symbol per trading
    day -- sorted by date so that any date range can be sliced in O(log n)
    without copying data.

    Used both for the market data and for the signals produced by
    `Strategy.prepare`. Requires a `timeframe` column of type Date (Datetime
    is truncated to Date) and a `symbol` column, unique together."""

    def __init__(self, df: pl.DataFrame) -> None:
        missing = [c for c in (DATE_COL, SYMBOL_COL) if c not in df.columns]
        if missing:
            raise ValueError(f"Panel is missing required column(s) {missing}")
        if df.schema[DATE_COL] != pl.Date:
            if not isinstance(df.schema[DATE_COL], pl.Datetime):
                raise ValueError(
                    f"Column {DATE_COL!r} must be Date, got {df.schema[DATE_COL]}"
                )
            df = df.with_columns(pl.col(DATE_COL).cast(pl.Date))
        if df[DATE_COL].null_count() or df[SYMBOL_COL].null_count():
            raise ValueError(
                f"Columns {DATE_COL!r} and {SYMBOL_COL!r} must not contain nulls"
            )
        self._df = df.sort([DATE_COL, SYMBOL_COL])
        # once sorted, duplicate keys are adjacent rows
        same_as_previous = (pl.col(DATE_COL) == pl.col(DATE_COL).shift(1)) & (
            pl.col(SYMBOL_COL) == pl.col(SYMBOL_COL).shift(1)
        )
        if self._df.select(same_as_previous.any()).item():
            raise ValueError(f"Panel has duplicate ({DATE_COL}, {SYMBOL_COL}) rows")

        self.dates: list[date] = (
            self._df[DATE_COL].unique(maintain_order=True).to_list()
        )
        starts = self._df[DATE_COL].search_sorted(
            pl.Series(self.dates, dtype=pl.Date), side="left"
        )
        # _offsets[i] is the first row of dates[i]; the extra last entry closes the final day
        self._offsets: list[int] = starts.to_list() + [self._df.height]

    @property
    def frame(self) -> pl.DataFrame:
        """The whole panel, validated and sorted by (timeframe, symbol)."""
        return self._df

    @property
    def columns(self) -> list[str]:
        return self._df.columns

    @property
    def height(self) -> int:
        return self._df.height

    def between(self, start: date | None, end: date) -> pl.DataFrame:
        """Rows with `start <= timeframe <= end` (`start=None`: from the beginning)."""
        i0 = 0 if start is None else bisect_left(self.dates, start)
        i1 = bisect_right(self.dates, end)
        if i1 <= i0:
            return self._df.clear()
        return self._df.slice(self._offsets[i0], self._offsets[i1] - self._offsets[i0])


class MarketView:
    """What a strategy is allowed to see at the close of trading day `date`:
    every accessor only ever returns rows with `timeframe <= date`.

    `lookback` arguments count *trading days* (days present in the market
    data) including today: `lookback=1` is today only, `lookback=21` roughly
    the last month, `None` all available history.

    The engine builds one view per simulated day; strategies must only use
    its public methods. Reaching into private attributes to read future rows
    is caught by the look-ahead guardrail tests."""

    __slots__ = ("_market", "_signals", "_index")

    def __init__(self, market: Panel, day: date, signals: Panel | None = None) -> None:
        index = bisect_left(market.dates, day)
        if index == len(market.dates) or market.dates[index] != day:
            raise ValueError(f"{day} is not a trading day in the market data")
        self._market = market
        self._signals = signals
        self._index = index

    @property
    def date(self) -> date:
        return self._market.dates[self._index]

    def trading_days(self, lookback: int | None = None) -> list[date]:
        """Trading days up to and including today, oldest first."""
        return self._market.dates[self._window_start_index(lookback) : self._index + 1]

    def history(
        self, columns: Sequence[str] | None = None, lookback: int | None = None
    ) -> pl.DataFrame:
        """Market data (prices, volumes and any other column of the dataset)
        for the last `lookback` trading days, sorted by (timeframe, symbol).
        `timeframe` and `symbol` are always included."""
        return self._read(self._market, columns, lookback)

    def today(self, columns: Sequence[str] | None = None) -> pl.DataFrame:
        """Today's market data, one row per symbol trading today."""
        return self.history(columns, lookback=1)

    def signals(
        self, columns: Sequence[str] | None = None, lookback: int | None = 1
    ) -> pl.DataFrame:
        """Rows of the strategy's `prepare()` output for the last `lookback`
        trading days (default: today only)."""
        if self._signals is None:
            raise RuntimeError(
                "No signals available: the strategy's prepare() returned None"
            )
        return self._read(self._signals, columns, lookback)

    def universe(self) -> list[str]:
        """Symbols investable today. If the market data has an `in_universe`
        column (point-in-time membership, e.g. index constituents as of each
        day), the symbols flagged true today; otherwise every symbol with
        market data today. Target weights on other symbols are rejected, so
        rank and pick within this list.

        Point-in-time membership is only as good as the data: symbols that
        stopped trading before the data was collected are usually missing
        altogether (survivorship bias, see `analysis.framework.survivorship`)."""
        if UNIVERSE_COL not in self._market.columns:
            return self.today([])[SYMBOL_COL].to_list()
        today = self.today([UNIVERSE_COL])
        return today.filter(pl.col(UNIVERSE_COL).fill_null(False))[SYMBOL_COL].to_list()

    def is_first_trading_day_of(self, period: Period) -> bool:
        """True on the first trading day of a new week/month/quarter/year
        (and on the very first day of the data). Useful for periodic
        rebalancing: `if not market.is_first_trading_day_of("month"): return None`."""
        if self._index == 0:
            return True
        previous = self._market.dates[self._index - 1]
        return period_key(previous, period) != period_key(self.date, period)

    def _window_start_index(self, lookback: int | None) -> int:
        if lookback is None:
            return 0
        if isinstance(lookback, bool) or not isinstance(lookback, int) or lookback < 1:
            raise ValueError(
                f"lookback must be a positive int or None, got {lookback!r}"
            )
        return max(0, self._index - lookback + 1)

    def _read(
        self, panel: Panel, columns: Sequence[str] | None, lookback: int | None
    ) -> pl.DataFrame:
        start = (
            None
            if lookback is None
            else self._market.dates[self._window_start_index(lookback)]
        )
        df = panel.between(start, self.date)
        if columns is None:
            return df
        if isinstance(columns, str):
            columns = [columns]
        missing = [c for c in columns if c not in panel.columns]
        if missing:
            raise KeyError(f"Unknown column(s) {missing}; available: {panel.columns}")
        keys = [DATE_COL, SYMBOL_COL]
        return df.select(keys + [c for c in columns if c not in keys])
