import gc
from abc import ABC, abstractmethod
from pathlib import Path
import polars as pl
import yaml

from etl.logger import get_logger

logger = get_logger(__name__)

DATAPLATFORM_ROOT = "./dataplatform"


class Model(ABC):
    def __init__(
        self, name: str, layer: str, partitioning_columns: list[str] = []
    ) -> None:
        super().__init__()
        self.name = name
        self.layer = layer
        self.partitioning_columns = partitioning_columns
        self._df = None

    @abstractmethod
    def _build(self) -> pl.DataFrame:
        pass

    def build(self) -> pl.DataFrame:
        logger.info("Building from source data model %s", self.name)
        self._df = self._build()
        logger.info(
            "%s/%s built: %d rows × %d cols — %.1f MB",
            self.layer,
            self.name,
            self._df.height,
            self._df.width,
            self._df.estimated_size("mb"),
        )
        return self._df

    @property
    def df(self) -> pl.DataFrame:
        if self._df is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.df accessed before build() or load_from_disk() was called."
            )
        return self._df

    def store(self):
        """Stores the dataframe as parquet under the appropriate data `layer` directory within
        a directory `model_name`. To set hive-partitioning provide the (ordered) list of column names
        to use for partitioning.

        Also includes a .yaml file with schema details of the generated dataframe"""

        layer_dir = Path(DATAPLATFORM_ROOT) / self.layer / self.name
        layer_dir.mkdir(parents=True, exist_ok=True)

        if self.partitioning_columns:
            self.df.write_parquet(layer_dir, partition_by=self.partitioning_columns)
        else:
            self.df.write_parquet(layer_dir / f"{self.name}.parquet")

        # create a schema file with information about the stored dataset
        schema_path = layer_dir / f"{self.name}_schema.yaml"
        schema_data = {
            "model": self.name,
            "layer": self.layer,
            "partitioned_by": self.partitioning_columns,
            "row_count": self.df.height,
            "columns": [
                {"name": col, "dtype": str(dtype)}
                for col, dtype in zip(self.df.columns, self.df.dtypes)
            ],
        }
        with open(schema_path, "w") as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "Stored %s/%s: %d rows × %d cols",
            self.layer,
            self.name,
            self.df.height,
            self.df.width,
        )

    def free(self):
        """Drops the in-memory DataFrame, releasing its Arrow buffer memory.

        CPython's reference counting will invoke Rust's Drop immediately when
        no other references exist. gc.collect() handles the rare case where a
        reference cycle would otherwise delay the release.

        After this call, accessing df will raise until build() or
        load_from_disk() is called again."""
        self._df = None
        gc.collect()

    def load_from_disk(self) -> pl.DataFrame:
        """Instead of computing the dataset from sources, reads it from
        the current version on disk as it gets saved by self.store"""

        logger.info("Loading from disk data model %s", self.name)
        layer_dir = Path(DATAPLATFORM_ROOT) / self.layer / self.name
        if self.partitioning_columns:
            glob_path = str(layer_dir / "**" / "*.parquet")
            self._df = pl.scan_parquet(glob_path, hive_partitioning=True).collect()
        else:
            self._df = pl.read_parquet(layer_dir / f"{self.name}.parquet")
        logger.info(
            "%s/%s loaded: %d rows × %d cols — %.1f MB",
            self.layer,
            self.name,
            self._df.height,
            self._df.width,
            self._df.estimated_size("mb"),
        )
        return self._df
