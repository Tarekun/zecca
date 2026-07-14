import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.model import Model


# --- minimal stubs (no real data needed) ---


class _MockModelV1(Model):
    """Returns a 3-row "a"/"b" frame, hive-partitioned by "b". The partition
    values are parametrized so consecutive runs land in different partition
    directories (e.g. simulating a source that drops/adds a category from
    one run to the next) -- this is what makes a stale-partition-file bug
    in store() actually show up."""

    def __init__(self, dataplatform_root, b_values=("x", "y", "z")):
        super().__init__(
            "mock", "test", partitioning_columns=["b"], dataplatform_root=dataplatform_root
        )
        self._b_values = b_values

    def _build(self) -> pl.LazyFrame:
        return pl.DataFrame({"a": [1, 2, 3], "b": list(self._b_values)}).lazy()


class _MockModelV2(Model):
    """Same model name as _MockModelV1 but with a different schema (new
    column "c", "b" dropped and no partitioning), simulating a schema
    change on the same model."""

    def __init__(self, dataplatform_root):
        super().__init__("mock", "test", dataplatform_root=dataplatform_root)

    def _build(self) -> pl.LazyFrame:
        return pl.DataFrame({"a": [10, 20], "c": [1.5, 2.5]}).lazy()


# --- tests ---


def test_store_full_refresh_does_not_duplicate_rows(tmp_path):
    # each run uses a different set of partition values, like a source whose
    # categories change day to day; a full refresh must still leave only the
    # latest run's rows on disk, with nothing accumulated from earlier runs
    runs = [
        ("x", "y", "z"),
        ("x", "y", "w"),
        ("x", "v", "z"),
    ]
    for b_values in runs:
        model = _MockModelV1(dataplatform_root=tmp_path, b_values=b_values)
        model.build()
        model.store()

    result = model.read_from_disk().collect()
    assert result.height == 3
    assert sorted(result["a"].to_list()) == [1, 2, 3]
    assert sorted(result["b"].to_list()) == sorted(runs[-1])


def test_store_full_refresh_updates_schema(tmp_path):
    for _ in range(2):
        model = _MockModelV1(dataplatform_root=tmp_path)
        model.build()
        model.store()

    new_model = _MockModelV2(dataplatform_root=tmp_path)
    new_model.build()
    new_model.store()

    result = new_model.read_from_disk().collect()
    assert result.height == 2
    assert result.columns == ["a", "c"]
    assert sorted(result["a"].to_list()) == [10, 20]

    # no leftover partition directories/files from the V1 runs
    model_dir = tmp_path / "test" / "mock"
    assert list(model_dir.glob("*.parquet")) == [model_dir / "mock.parquet"]
    assert not any(p.is_dir() and p.name.startswith("b=") for p in model_dir.iterdir())
