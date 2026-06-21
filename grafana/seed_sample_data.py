"""Generate sample OHLCV parquet data for the Grafana PoC.

Produces hive-partitioned parquet that mirrors the schema of the project's
``silver/candles_daily`` model (a subset of its columns — enough to drive a
candlestick dashboard). The output layout matches what ``etl.transformation.model.Model.store``
writes, so the same Grafana query works against real pipeline output.

Layout written:
    grafana/data/candles_daily/year=YYYY/month=M/*.parquet

Run:
    .venv/Scripts/python grafana/seed_sample_data.py
"""

from __future__ import annotations

import math
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl

OUT_DIR = Path(__file__).parent / "data" / "candles_daily"
SYMBOLS = {"AAPL": 180.0, "MSFT": 410.0, "TSLA": 250.0}
YEARS_OF_HISTORY = 2
SEED = 42


def _business_days(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d += timedelta(days=1)
    return days


def _simulate(symbol: str, start_price: float, days: list[date]) -> pl.DataFrame:
    """Geometric-brownian-motion daily candles with a plausible intraday range."""
    rng = random.Random(f"{SEED}-{symbol}")
    rows = []
    prev_close = start_price
    for d in days:
        drift, vol = 0.0003, 0.018
        ret = drift + vol * rng.gauss(0, 1)
        open_ = prev_close
        close = max(1.0, open_ * math.exp(ret))
        high = max(open_, close) * (1 + abs(rng.gauss(0, 0.006)))
        low = min(open_, close) * (1 - abs(rng.gauss(0, 0.006)))
        volume = int(rng.uniform(5_000_000, 80_000_000))
        rows.append(
            {
                "timeframe": d,
                "year": d.year,
                "month": d.month,
                "symbol": symbol,
                "open": round(open_, 2),
                "close": round(close, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "volume": volume,
            }
        )
        prev_close = close
    return pl.DataFrame(rows).with_columns(
        pl.col("timeframe").cast(pl.Date),
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int32),
    )


def main() -> None:
    end = date.today()
    start = end - timedelta(days=365 * YEARS_OF_HISTORY)
    days = _business_days(start, end)

    frames = [_simulate(sym, price, days) for sym, price in SYMBOLS.items()]
    df = pl.concat(frames).sort(["symbol", "timeframe"])

    if OUT_DIR.exists():
        # polars refuses to write into a non-empty hive root; clear it first
        for p in sorted(OUT_DIR.rglob("*"), reverse=True):
            p.rmdir() if p.is_dir() else p.unlink()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df.write_parquet(OUT_DIR, partition_by=["year", "month"])
    print(
        f"Wrote {df.height} rows for {len(SYMBOLS)} symbols "
        f"({start} -> {end}) to {OUT_DIR}"
    )


if __name__ == "__main__":
    main()
