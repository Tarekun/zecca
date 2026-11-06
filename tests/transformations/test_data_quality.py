import datetime
import logging
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.model import Model
from etl.transformation.quality_checks import (
    DataQualityError,
    accepted_values,
    column_comparison,
    foreign_key,
    freshness,
    in_range,
    is_finite,
    list_length,
    matches_regex,
    no_gaps,
    no_survivorship_bias,
    not_empty,
    not_null,
    rejected_values,
    unique,
)


# --- minimal stubs (no real data needed) ---


class _StubModel(Model):
    def __init__(self, dataplatform_root, name, build_df, quality_checks=[]):
        super().__init__(
            name,
            "test",
            dataplatform_root=dataplatform_root,
            quality_checks=quality_checks,
        )
        self._build_df = build_df

    def _build(self) -> pl.LazyFrame:
        return self._build_df.lazy()


def _raising_check(lf: pl.LazyFrame) -> pl.LazyFrame:
    raise ValueError("boom")


class _TargetStub(Model):
    """A Model whose read_from_disk() returns a fixed DataFrame directly,
    so foreign_key tests don't need to actually persist parquet files."""

    def __init__(self, df):
        super().__init__("target", "test")
        self._df = df

    def _build(self) -> pl.LazyFrame:
        return self._df.lazy()

    def read_from_disk(self) -> pl.LazyFrame:
        return self._df.lazy()


# --- tests ---


def test_passing_checks_do_not_raise(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
        quality_checks=[not_null(["a", "b"])],
    )
    model.build()
    model.run_quality_checks()


def test_failing_not_null_raises_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [1, None], "b": [None, None]}),
        quality_checks=[not_null(["a", "b"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "not_null_a_b.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 1
    assert violations["violating_rows"].to_list() == [2]


def test_raising_check_reports_error_without_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "raising",
        pl.DataFrame({"a": [1]}),
        quality_checks=[_raising_check],
    )
    model.build()
    with pytest.raises(DataQualityError, match="raised ValueError: boom"):
        model.run_quality_checks()

    assert not (tmp_path / "test_outputs").exists()


def test_no_declared_checks_is_a_noop(tmp_path):
    model = _StubModel(tmp_path, "no_checks", pl.DataFrame({"a": [None]}))
    model.build()
    model.run_quality_checks()


def test_raises_if_called_before_build(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [1]}),
        quality_checks=[not_null("a")],
    )
    with pytest.raises(RuntimeError, match="before build"):
        model.run_quality_checks()


def test_not_null_accepts_a_single_column_name(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [1, None], "b": [None, None]}),
        quality_checks=[not_null("a")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "not_null_a.csv"
    assert csv_path.exists()
    assert pl.read_csv(csv_path)["violating_rows"].to_list() == [1]


def test_unique_passes_when_column_combination_has_no_duplicates(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "x"]}),
        quality_checks=[unique(["a", "b"])],
    )
    model.build()
    model.run_quality_checks()


def test_unique_fails_on_duplicate_column_combination_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [1, 1, 1, 2], "b": ["x", "x", "y", "x"]}),
        quality_checks=[unique(["a", "b"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "unique_a_b.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 2
    assert set(violations["a"].to_list()) == {1}
    assert set(violations["b"].to_list()) == {"x"}


def test_unique_accepts_a_single_column_name(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [1, 1, 2]}),
        quality_checks=[unique("a")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()


def test_accepted_values_passes_when_column_only_has_allowed_values(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": ["x", "y", "x"]}),
        quality_checks=[accepted_values("a", ["x", "y"])],
    )
    model.build()
    model.run_quality_checks()


def test_accepted_values_fails_on_value_outside_allowlist_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": ["x", "y", "z", "z"]}),
        quality_checks=[accepted_values("a", ["x", "y"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "accepted_values_a.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 2
    assert set(violations["a"].to_list()) == {"z"}


def test_rejected_values_passes_when_column_has_none_of_the_blocked_values(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": ["x", "y", "x"]}),
        quality_checks=[rejected_values("a", ["z"])],
    )
    model.build()
    model.run_quality_checks()


def test_rejected_values_fails_on_blocked_value_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": ["x", "y", "z", "z"]}),
        quality_checks=[rejected_values("a", ["z"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "rejected_values_a.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 2
    assert set(violations["a"].to_list()) == {"z"}


def test_list_length_passes_when_all_lists_have_the_expected_length(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [[1, 2], [3, 4]]}),
        quality_checks=[list_length("a", 2)],
    )
    model.build()
    model.run_quality_checks()


def test_list_length_fails_on_wrong_length_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [[1, 2], [3], [4, 5, 6]]}),
        quality_checks=[list_length("a", 2)],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "list_length_a.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 2
    assert sorted(violations["actual_length"].to_list()) == [1, 3]


def test_foreign_key_passes_when_every_value_exists_in_target(tmp_path):
    target = _TargetStub(pl.DataFrame({"id": [1, 2, 3]}))
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"parent_id": [1, 2, 2]}),
        quality_checks=[foreign_key(["parent_id"], target, ["id"])],
    )
    model.build()
    model.run_quality_checks()


def test_foreign_key_fails_on_value_missing_from_target_and_writes_csv(tmp_path):
    target = _TargetStub(pl.DataFrame({"id": [1, 2]}))
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"parent_id": [1, 2, 3, 3]}),
        quality_checks=[foreign_key(["parent_id"], target, ["id"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "foreign_key_parent_id.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 2
    assert set(violations["parent_id"].to_list()) == {3}


def test_foreign_key_defaults_target_columns_to_the_same_names(tmp_path):
    target = _TargetStub(pl.DataFrame({"parent_id": [1, 2]}))
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"parent_id": [1, 2, 3]}),
        quality_checks=[foreign_key(["parent_id"], target)],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()


def test_foreign_key_accepts_a_model_class(tmp_path):
    target_df = pl.DataFrame({"id": [1, 2]})

    class _TargetClass(_TargetStub):
        def __init__(self):
            super().__init__(target_df)

    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"parent_id": [1, 2, 3]}),
        quality_checks=[foreign_key(["parent_id"], _TargetClass, ["id"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()


def test_column_comparison_passes_when_relation_holds(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"low": [1, 2], "high": [2, 2]}),
        quality_checks=[column_comparison("low", "<=", "high")],
    )
    model.build()
    model.run_quality_checks()


def test_column_comparison_fails_when_relation_is_violated_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"low": [1, 3, 5], "high": [2, 2, 5]}),
        quality_checks=[column_comparison("low", "<=", "high")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = (
        tmp_path / "test_outputs" / "test" / "dirty" / "column_comparison_low_le_high.csv"
    )
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations.height == 1
    assert violations["low"].to_list() == [3]
    assert violations["high"].to_list() == [2]


def test_column_comparison_rejects_unsupported_operator():
    with pytest.raises(ValueError, match="Unsupported comparison operator"):
        column_comparison("low", "=~", "high")


def test_in_range_requires_at_least_one_bound():
    with pytest.raises(ValueError, match="at least one of min_value or max_value"):
        in_range("a")


def test_in_range_passes_when_within_bounds(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [0, 50, 100]}),
        quality_checks=[in_range("a", min_value=0, max_value=100)],
    )
    model.build()
    model.run_quality_checks()


def test_in_range_fails_outside_bounds_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [-1, 50, 101]}),
        quality_checks=[in_range("a", min_value=0, max_value=100)],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "in_range_a.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert sorted(violations["a"].to_list()) == [-1, 101]


def test_in_range_only_enforces_the_given_bound(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [-5, 0, 5]}),
        quality_checks=[in_range("a", min_value=0)],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()


def test_freshness_passes_when_recent(tmp_path):
    today = datetime.date.today()
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"d": [today - datetime.timedelta(days=1), today]}),
        quality_checks=[freshness("d", datetime.timedelta(days=5))],
    )
    model.build()
    model.run_quality_checks()


def test_freshness_fails_when_stale_and_writes_csv(tmp_path):
    today = datetime.date.today()
    stale_date = today - datetime.timedelta(days=10)
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"d": [today - datetime.timedelta(days=20), stale_date]}),
        quality_checks=[freshness("d", datetime.timedelta(days=5))],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "freshness_d.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations["latest"].to_list() == [stale_date.isoformat()]


def test_not_empty_passes_when_rows_present(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [1]}),
        quality_checks=[not_empty()],
    )
    model.build()
    model.run_quality_checks()


def test_not_empty_fails_on_empty_frame_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)}),
        quality_checks=[not_empty()],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "not_empty.csv"
    assert csv_path.exists()
    assert pl.read_csv(csv_path)["row_count"].to_list() == [0]


def test_is_finite_passes_when_values_are_finite_or_null(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"a": [1.0, None, -2.5]}),
        quality_checks=[is_finite("a")],
    )
    model.build()
    model.run_quality_checks()


def test_is_finite_fails_on_inf_and_nan_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"a": [1.0, float("inf"), float("nan"), float("-inf")]}),
        quality_checks=[is_finite("a")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 3 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "is_finite_a.csv"
    assert csv_path.exists()
    assert pl.read_csv(csv_path).height == 3


def test_matches_regex_passes_when_values_match_or_null(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame({"ticker": ["AAPL", "BRK.B", None]}),
        quality_checks=[matches_regex("ticker", r"[A-Za-z0-9.\-]+")],
    )
    model.build()
    model.run_quality_checks()


def test_matches_regex_fails_on_non_matching_value_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame({"ticker": ["AAPL", "bad ticker", ""]}),
        quality_checks=[matches_regex("ticker", r"[A-Za-z0-9.\-]+")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 2 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "matches_regex_ticker.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert sorted(violations["ticker"].fill_null("").to_list()) == ["", "bad ticker"]


def test_no_gaps_passes_when_dates_are_continuous_per_group(tmp_path):
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame(
            {
                "cik": [1, 1, 1, 2, 2],
                "d": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                    datetime.date(2024, 1, 3),
                    datetime.date(2024, 2, 1),
                    datetime.date(2024, 2, 2),
                ],
            }
        ),
        quality_checks=[no_gaps("d", "cik")],
    )
    model.build()
    model.run_quality_checks()


def test_no_gaps_fails_on_missing_date_and_writes_csv(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame(
            {
                "cik": [1, 1, 1],
                "d": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                    datetime.date(2024, 1, 4),
                ],
            }
        ),
        quality_checks=[no_gaps("d", "cik")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "no_gaps_d_cik.csv"
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations["d"].to_list() == [datetime.date(2024, 1, 3).isoformat()]


def test_no_survivorship_bias_passes_when_a_symbol_disappears_early(
    tmp_path, caplog, monkeypatch
):
    # the "etl" logger disables propagation to the root logger in production,
    # which would otherwise keep caplog (attached at the root) from seeing it
    monkeypatch.setattr(logging.getLogger("etl"), "propagate", True)
    model = _StubModel(
        tmp_path,
        "clean",
        pl.DataFrame(
            {
                "symbol": ["A", "A", "A", "B", "B"],
                "timeframe": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                    datetime.date(2024, 1, 3),
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                ],
            }
        ),
        quality_checks=[no_survivorship_bias("timeframe", "symbol")],
    )
    model.build()
    with caplog.at_level("INFO"):
        model.run_quality_checks()

    assert "1 symbol(s)" in caplog.text


def test_no_survivorship_bias_fails_when_every_symbol_reaches_the_max_timeframe(
    tmp_path,
):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame(
            {
                "symbol": ["A", "A", "B", "B"],
                "timeframe": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                ],
            }
        ),
        quality_checks=[no_survivorship_bias("timeframe", "symbol")],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = (
        tmp_path
        / "test_outputs"
        / "test"
        / "dirty"
        / "no_survivorship_bias_symbol.csv"
    )
    assert csv_path.exists()
    violations = pl.read_csv(csv_path)
    assert violations["symbols_never_reappearing"].to_list() == [0]


def test_no_gaps_accepts_multiple_group_by_columns(tmp_path):
    model = _StubModel(
        tmp_path,
        "dirty",
        pl.DataFrame(
            {
                "cik": [1, 1, 1],
                "metric": ["shares", "shares", "shares"],
                "d": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 2),
                    datetime.date(2024, 1, 4),
                ],
            }
        ),
        quality_checks=[no_gaps("d", ["cik", "metric"])],
    )
    model.build()
    with pytest.raises(DataQualityError, match="found 1 violating row"):
        model.run_quality_checks()

    csv_path = tmp_path / "test_outputs" / "test" / "dirty" / "no_gaps_d_cik_metric.csv"
    assert csv_path.exists()
