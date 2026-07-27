import polars as pl
from typing import Callable, Literal

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
