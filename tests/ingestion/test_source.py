import json
import sys
from pathlib import Path

import polars as pl
import yaml

sys.path.append(str(Path(__file__).parents[2]))
from etl.ingestion.source import DictSource, TableSource

# --- minimal stubs (no real external source needed) ---


class _MockSingleItemSource(DictSource):
    """Returns whatever payload it's constructed with, simulating a source
    whose whole result is one JSON-serializable object (e.g. company_tickers'
    array of CIK/ticker/name triples)."""

    def __init__(self, dataplatform_root, payload, **kwargs):
        super().__init__("mock_single", dataplatform_root=dataplatform_root, **kwargs)
        self._payload = payload

    def load(self, **kwargs):
        return self._payload


class _MockCollectionSource(DictSource):
    """Simulates a source that writes one file per item directly during
    load() (like the SEC company-facts download), so `_persist()` is a
    no-op -- mirrors `SecCompanyFacts`."""

    def __init__(self, dataplatform_root, items):
        super().__init__("mock_collection", dataplatform_root=dataplatform_root)
        self._items = items

    def load(self, **kwargs):
        self._root_dir.mkdir(parents=True, exist_ok=True)
        for key, payload in self._items.items():
            with open(self._root_dir / f"{key}.{self.format}", "w") as f:
                json.dump(payload, f)

    def _persist(self, data) -> None:
        pass


class _MockTableSource(TableSource):
    """Returns whatever DataFrame it's constructed with each time load() is
    called, simulating a source that pulls a fresh batch of rows per run."""

    def __init__(self, dataplatform_root, batches, partitioning_columns=["b"]):
        super().__init__(
            "mock",
            key_columns=["a"],
            partitioning_columns=partitioning_columns,
            dataplatform_root=dataplatform_root,
        )
        self._batches = iter(batches)

    def load(self, **kwargs) -> pl.DataFrame:
        return next(self._batches)


# --- tests ---


def test_ingest_persists_and_read_from_disk_returns_it_back(tmp_path):
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "x", "y"]})
    source = _MockTableSource(tmp_path, batches=[df])

    result = source.ingest()

    assert result.equals(df)
    on_disk = source.read_from_disk().collect().sort("a")
    assert on_disk["a"].to_list() == [1, 2, 3]
    assert (tmp_path / "raw" / "mock" / "b=x").is_dir()
    assert (tmp_path / "raw" / "mock" / "b=y").is_dir()


def test_ingest_upserts_on_key_columns_across_runs(tmp_path):
    source = _MockTableSource(
        tmp_path,
        batches=[
            pl.DataFrame({"a": [1, 2], "b": ["x", "x"]}),
            pl.DataFrame({"a": [2, 3], "b": ["x", "x"]}),
        ],
    )

    source.ingest()
    source.ingest()

    result = source.read_from_disk().collect().sort("a")
    assert result["a"].to_list() == [1, 2, 3]


def test_ingest_with_empty_load_is_a_noop(tmp_path):
    source = _MockTableSource(tmp_path, batches=[pl.DataFrame()])

    source.ingest()

    assert not source.exists()


def test_read_from_disk_without_partitioning(tmp_path):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    source = _MockTableSource(tmp_path, batches=[df], partitioning_columns=[])

    source.ingest()

    result = source.read_from_disk().collect().sort("a")
    assert result["a"].to_list() == [1, 2]
    assert (tmp_path / "raw" / "mock" / "data.parquet").is_file()


def test_dict_source_single_item_roundtrips_as_json(tmp_path):
    payload = [{"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}]
    source = _MockSingleItemSource(tmp_path, payload=payload)

    result = source.ingest()

    assert result == payload
    assert source.read_from_disk() == [payload]
    path = tmp_path / "raw" / "mock_single" / "mock_single.json"
    assert path.is_file()
    assert json.loads(path.read_text()) == payload


def test_dict_source_single_item_can_use_yaml_format(tmp_path):
    payload = {"a": 1, "b": [1, 2, 3]}
    source = _MockSingleItemSource(tmp_path, payload=payload, format="yaml")

    source.ingest()

    assert source.read_from_disk() == [payload]
    path = tmp_path / "raw" / "mock_single" / "mock_single.yaml"
    assert path.is_file()
    assert yaml.safe_load(path.read_text()) == payload


def test_dict_source_collection_writes_one_file_per_item(tmp_path):
    items = {
        "CIK0000320193": {"cik": 320193, "entityName": "Apple Inc."},
        "CIK0000789019": {"cik": 789019, "entityName": "Microsoft Corp."},
    }
    source = _MockCollectionSource(tmp_path, items=items)

    source.ingest()

    stored = {Path(p).stem: source.read_item(p) for p in source.items()}
    assert stored == items
    assert (tmp_path / "raw" / "mock_collection" / "CIK0000320193.json").is_file()


def test_dict_source_collection_items_empty_before_ingest(tmp_path):
    source = _MockCollectionSource(tmp_path, items={})
    assert source.items() == []
