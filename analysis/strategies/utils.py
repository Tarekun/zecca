from datetime import date
import polars as pl
from typing import Literal


def prices_on(df: pl.DataFrame, day: date) -> dict[str, float]:
    """The median (high+low)/2 execution price for every symbol on `day`,
    used to mark and transact positions."""

    today = df.filter(pl.col("timeframe") == day).with_columns(
        ((pl.col("high") + pl.col("low")) / 2).alias("median_price")
    )
    return dict(zip(today["symbol"], today["median_price"]))


def period_key(day: date, period: Literal["weekly", "monthly", "quarterly"]):
    if period == "weekly":
        iso = day.isocalendar()
        return (iso[0], iso[1])
    elif period == "monthly":
        return (day.year, day.month)
    else:
        return (day.year, (day.month - 1) // 3)
