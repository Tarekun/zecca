import math

import polars as pl


def safe_log_return(entry_value: pl.Expr, current_value: pl.Expr) -> pl.Expr:
    """Compute the log return between two prices.
    Evaluates to `ln(entry_value / current_value)` when both prices are strictly
    positive; returns `None` otherwise to avoid taking the log of a non-positive
    ratio.

    Returns:
        Polars expression yielding the log return, or `None` when either input is
        non-positive.
    """

    return (
        pl.when((entry_value > 0) & (current_value > 0))
        .then((entry_value / current_value).log(math.e))
        .otherwise(None)
    )


def safe_return(current: pl.Expr, initial: pl.Expr) -> pl.Expr:
    """Compute the percentage return: `(current - initial) / initial`.
    Returns ``None`` when *initial* is zero to guard against division by zero.

    Returns:
        Polars expression yielding the return ratio, or `None` when *initial* is zero.
    """

    return pl.when(initial != 0).then((current - initial) / initial).otherwise(None)


# TODO deprecare
def safe_div(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """Divide two expressions, returning ``None`` when the denominator is zero.

    Mirrors the ``safe_div`` macro in ``math.sql``.

    Args:
        numerator: Expression for the dividend.
        denominator: Expression for the divisor.

    Returns:
        Polars expression yielding the quotient, or ``None`` when *denominator* is zero.
    """
    return pl.when(denominator != 0).then(numerator / denominator).otherwise(None)


def rolling_avg(col: pl.Expr, window: int) -> pl.Expr:
    """Compute a rolling mean with a window of exactly `window` rows.
    Call `.over(partition)` on the returned expression to restrict computation to a partition.

    Returns:
        Polars rolling-mean expression.
    """
    return col.rolling_mean(window_size=window, min_samples=1)


def volatility(col: pl.Expr, window: int) -> pl.Expr:
    """Compute the rolling sample standard deviation of *col* over a window of *window* rows.

    Two or more non-null rows are required per window; windows with fewer yield ``None``.
    Call `.over(partition)` on the returned expression to restrict computation to a partition.

    Args:
        col: Expression whose volatility to compute (typically the 1-day log return).
        window: Total window size passed directly to ``rolling_std``.

    Returns:
        Polars rolling sample standard-deviation expression (ddof=1).
    """
    return col.rolling_std(window_size=window, min_samples=2, ddof=1)


def relative_strength_index(price_diff: pl.Expr, window: int) -> pl.Expr:
    """Compute the Relative Strength Index (RSI) over a rolling window.

    Uses the formula ``100 - 100 / (1 + avg_gain / avg_loss)`` where average gains
    and losses are computed separately over the window (zero-substituted where the
    condition is not met). Call ``.over(partition)`` on the returned expression to
    restrict computation to a partition.

    Mirrors the ``relative_strength_index`` macro in ``formulas.sql``.

    Args:
        price_diff: Expression of close-price 1-day differences
            (``close_t - close_{t-1}``).
        window: Total window size passed directly to ``rolling_mean``.

    Returns:
        Polars expression yielding RSI in the range [0, 100].
    """
    avg_gain = (
        pl.when(price_diff > 0)
        .then(price_diff)
        .otherwise(pl.lit(0.0))
        .rolling_mean(window_size=window, min_samples=1)
    )
    avg_loss = (
        pl.when(price_diff < 0)
        .then(price_diff.abs())
        .otherwise(pl.lit(0.0))
        .rolling_mean(window_size=window, min_samples=1)
    )
    return 100 - 100 / (1 + avg_gain / avg_loss)
