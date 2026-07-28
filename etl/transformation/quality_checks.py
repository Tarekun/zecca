import datetime
import operator
import polars as pl
from typing import Any, Callable, Literal

# A data quality check is any function taking the model's built LazyFrame and
# returning the rows that violate it (empty result = check passes). Raising
# instead of returning is also a valid failure (see run_data_quality_checks).
DataQualityCheck = Callable[[pl.LazyFrame], pl.LazyFrame | pl.DataFrame]


def not_null(columns: str | list[str]) -> DataQualityCheck:
    """Builds a check that fails on any row where one of `columns` is null."""
    columns = [columns] if isinstance(columns, str) else list(columns)

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.select(
            pl.any_horizontal(pl.col(c).is_null() for c in columns)
            .sum()
            .alias("violating_rows")
        ).filter(pl.col("violating_rows") > 0)

    check.__name__ = f"not_null_{'_'.join(columns)}"
    return check


def list_length(column: str, length: int) -> DataQualityCheck:
    """Builds a check that fails on any row where the list in `column` does
    not have exactly `length` elements.

    Reports each violating row's actual length rather than the list itself,
    since the raw list can't be written to the checks' output CSV."""

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.select(pl.col(column).list.len().alias("actual_length")).filter(
            pl.col("actual_length") != length
        )

    check.__name__ = f"list_length_{column}"
    return check


def in_range(
    column: str, min_value: Any = None, max_value: Any = None
) -> DataQualityCheck:
    """Builds a check that fails on any row where `column`'s value is below
    `min_value` or above `max_value` (inclusive bounds). At least one of the
    two must be given."""
    if min_value is None and max_value is None:
        raise ValueError("in_range requires at least one of min_value or max_value")

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        conditions = []
        if min_value is not None:
            conditions.append(pl.col(column) < min_value)
        if max_value is not None:
            conditions.append(pl.col(column) > max_value)
        return lf.filter(pl.any_horizontal(conditions))

    check.__name__ = f"in_range_{column}"
    return check


def freshness(column: str, max_age: datetime.timedelta) -> DataQualityCheck:
    """Builds a check that fails if the most recent value in `column` is
    older than `max_age` relative to now, i.e. the data looks stale.

    `column` may hold Date or Datetime values."""

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        dtype = lf.collect_schema()[column]
        now = datetime.date.today() if dtype == pl.Date else datetime.datetime.now()
        return lf.select(pl.col(column).max().alias("latest")).filter(
            (pl.lit(now) - pl.col("latest")) > max_age
        )

    check.__name__ = f"freshness_{column}"
    return check


def accepted_values(column: str, values: list) -> DataQualityCheck:
    """Builds a check that fails on any row where `column`'s value is not one
    of `values`."""

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.filter(~pl.col(column).is_in(values))

    check.__name__ = f"accepted_values_{column}"
    return check


def rejected_values(column: str, values: list) -> DataQualityCheck:
    """Builds a check that fails on any row where `column`'s value is one of
    `values`."""

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.filter(pl.col(column).is_in(values))

    check.__name__ = f"rejected_values_{column}"
    return check


def column_comparison(
    column_a: str,
    op: Literal["<", "<=", ">", ">=", "==", "!="],
    column_b: str,
) -> DataQualityCheck:
    """Builds a check that fails on any row where `column_a <op> column_b`
    does not hold, e.g. `column_comparison("low", "<=", "high")`.

    `op` is one of "<", "<=", ">", ">=", "==", "!="."""

    COMPARISON_OPS = {
        "<": (operator.lt, "lt"),
        "<=": (operator.le, "le"),
        ">": (operator.gt, "gt"),
        ">=": (operator.ge, "ge"),
        "==": (operator.eq, "eq"),
        "!=": (operator.ne, "ne"),
    }
    compare, op_name = COMPARISON_OPS[op]

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.filter(~compare(pl.col(column_a), pl.col(column_b)))

    check.__name__ = f"column_comparison_{column_a}_{op_name}_{column_b}"
    return check


def foreign_key(
    columns: str | list[str],
    target_model,
    target_columns: str | list[str] | None = None,
) -> DataQualityCheck:
    """Builds a check that fails on any row whose `columns` combination has no
    matching row in `target_model` under `target_columns` (defaults to
    `columns`), i.e. `columns` is not a valid foreign key into that table.

    `target_model` is a Model instance, or a Model subclass constructible
    with no arguments; its data is read from disk lazily, only when the
    check actually runs."""
    columns = [columns] if isinstance(columns, str) else list(columns)
    if target_columns is None:
        target_columns = columns
    else:
        target_columns = (
            [target_columns]
            if isinstance(target_columns, str)
            else list(target_columns)
        )

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        model = target_model() if isinstance(target_model, type) else target_model
        target_lf = (
            model.read_from_disk()
            .select(target_columns)
            .rename(dict(zip(target_columns, columns)))
            .unique()
        )
        return lf.join(target_lf, on=columns, how="anti")

    check.__name__ = f"foreign_key_{'_'.join(columns)}"
    return check


def unique(columns: str | list[str]) -> DataQualityCheck:
    """Builds a check that fails on any row whose combination of `columns` is
    shared by more than one row, i.e. `columns` is not a unique (composite) key."""
    columns = [columns] if isinstance(columns, str) else list(columns)

    def check(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.filter(pl.len().over(columns) > 1)

    check.__name__ = f"unique_{'_'.join(columns)}"
    return check


class DataQualityError(Exception):
    """Raised by Model.run_data_quality_checks() when one or more of the
    model's declared `data_quality_checks` fail."""

    def __init__(self, model_id: str, failures: list[str]) -> None:
        self.model_id = model_id
        self.failures = failures
        super().__init__(
            f"{model_id} failed {len(failures)} data quality check(s):\n"
            + "\n".join(f"  - {failure}" for failure in failures)
        )
