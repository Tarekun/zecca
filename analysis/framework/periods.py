from datetime import date
from typing import Literal

Period = Literal["week", "month", "quarter", "year"]


def period_key(day: date, period: Period) -> tuple[int, ...]:
    """Identifies the calendar `period` that `day` belongs to: two days share
    a key iff they fall in the same ISO week / month / quarter / year."""
    if period == "week":
        iso = day.isocalendar()
        return (iso[0], iso[1])
    if period == "month":
        return (day.year, day.month)
    if period == "quarter":
        return (day.year, (day.month - 1) // 3)
    if period == "year":
        return (day.year,)
    raise ValueError(f"Unknown period {period!r}")
