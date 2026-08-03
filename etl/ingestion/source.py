import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal

import polars as pl
import yaml

from etl.logger import get_logger
from etl.utils import upsert_df

logger = get_logger(__name__)

DEFAULT_DATAPLATFORM_ROOT = "./dataplatform"


class Source(ABC):
    """Abstract base for an external data source: something downloaded once
    and made available to the rest of the pipeline.

    A concrete source only implements `load()` (how to fetch fresh data) and
    `read_from_disk()` (how to hand it back for downstream consumption);
    persisting what `load()` returns is handled by `ingest()`, via the
    `_persist()` hook.
    """

    def __init__(
        self,
        name: str,
        layer: str = "raw",
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
        incremental: bool = False,
    ) -> None:
        super().__init__()
        self.name = name
        self.layer = layer
        self.dataplatform_root = dataplatform_root
        self.incremental = incremental

    @property
    def id(self) -> str:
        return f"{self.layer}.{self.name}"

    @property
    def _root_dir(self) -> Path:
        return Path(self.dataplatform_root) / self.layer / self.name

    @abstractmethod
    def load(self, **kwargs) -> Any:
        """Fetches fresh data from the external source and returns it. Must
        not touch disk directly -- `ingest()` persists the result via
        `_persist()`."""

    @abstractmethod
    def _persist(self, data: Any) -> None:
        """Writes freshly loaded `data` to disk, merging with whatever is
        already stored under this source's location."""

    @abstractmethod
    def read_from_disk(self) -> Any:
        """Accessor over whatever is currently stored on disk for this
        source, for downstream filtering/consumption."""

    def ingest(self, **kwargs) -> Any:
        """Runs load() and persists the result -- the source's equivalent of
        Model.build_store_free()."""

        logger.info(f"Starting {self.id} ingestion...")
        data = self.load(**kwargs)
        self._persist(data)
        return data


class TableLike(Source):
    """A Source whose data is row/columnar (e.g. OHLCV candles): `load()`
    returns a DataFrame that `ingest()` merges into an on-disk, optionally
    hive-partitioned, parquet store keyed by `key_columns`."""

    def __init__(
        self,
        name: str,
        key_columns: list[str],
        partitioning_columns: list[str] = [],
        layer: str = "raw",
        **kwargs,
    ) -> None:
        super().__init__(name, layer, **kwargs)
        self.key_columns = key_columns
        self.partitioning_columns = partitioning_columns

    def _persist(self, data: pl.DataFrame) -> None:
        if data.is_empty():
            logger.info("%s: nothing to persist, load() returned no rows", self.id)
            return
        upsert_df(
            data,
            self.name,
            str(Path(self.dataplatform_root) / self.layer),
            self.key_columns,
            self.partitioning_columns or None,
        )

    def read_from_disk(self) -> pl.LazyFrame:
        glob_pattern = (
            "*.parquet"
            if not self.partitioning_columns
            else "/".join(f"{c}=*" for c in self.partitioning_columns) + "/*.parquet"
        )
        files = sorted(str(p) for p in self._root_dir.glob(glob_pattern))
        return pl.concat(
            [
                self._normalize(pl.scan_parquet(f, extra_columns="ignore"))
                for f in files
            ],
            how="vertical",
        )

    def _normalize(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Hook for reconciling per-file dtype/column drift (e.g. legacy
        files written by an older pandas/pyarrow version) before
        concatenation. Polars enforces a single dtype per column when
        scanning multiple parquet files as one glob, so mismatched files
        must be normalized individually first. Default is a no-op."""
        return lf


class DictLike(Source):
    """A Source whose data is JSON-serializable (a dict or list), stored on
    disk as one file per "item".

    A single-item source (the default behavior of `_persist`/
    `read_from_disk`) keeps one file for its whole payload, named after the
    source itself and living directly under the layer directory (e.g.
    `raw/company_tickers.json`, one array covering every ticker).

    A collection source keeps one file per item inside a directory named
    after the source (e.g. `raw/sec/CIK0000320193.json`, one file per
    company); such a source overrides `_persist`/`read_from_disk` and uses
    `item()`/`items()` to access individual entries instead.
    """

    def __init__(
        self,
        name: str,
        layer: str = "raw",
        format: Literal["json", "yaml"] = "json",
        **kwargs,
    ) -> None:
        super().__init__(name, layer, **kwargs)
        self.format = format

    def _persist(self, data: Any) -> None:
        # TODO support items splitting somehow
        root = Path(self.dataplatform_root) / self.layer / self.name
        root.mkdir(parents=True, exist_ok=True)

        with open(root / f"{self.name}.{self.format}", "w") as f:
            if self.format == "yaml":
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            else:
                json.dump(data, f)

    def read_item(self, key: str) -> Any:
        with open(key) as f:
            return yaml.safe_load(f) if self.format == "yaml" else json.load(f)

    def read_from_disk(self) -> Any:
        print(self.items())
        return [self.read_item(item) for item in self.items()]

    def items(self) -> list[str]:
        """Keys of every item currently stored under this collection
        source."""
        if not self._root_dir.exists():
            return []
        return sorted(str(p) for p in self._root_dir.glob(f"*.{self.format}"))
