from abc import ABC, abstractmethod
from pathlib import Path
import polars as pl
from typing import Any


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

    def exists(self) -> bool:
        """Whether anything has ever been persisted for this source."""
        return self._root_dir.exists()

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


class TableSource(Source):
    """A Source whose data is row/columnar (e.g. OHLCV candles): `load()`
    returns a DataFrame that `ingest()` merges into an on-disk, optionally
    hive-partitioned, parquet store keyed by `key_columns`."""

    def __init__(
        self,
        name: str,
        key_columns: list[str],
        partitioning_columns: list[str] = [],
        layer: str = "raw",
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(name, layer, dataplatform_root)
        self.key_columns = key_columns
        self.partitioning_columns = partitioning_columns

    def _persist(self, data: pl.DataFrame) -> None:
        if data.is_empty():
            logger.info("%s: nothing to persist, load() returned no rows", self.id)
            return
        upsert_df(
            data,
            self.name,
            self.dataplatform_root,
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
