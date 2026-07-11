import gc
import graphlib
import multiprocessing
from abc import ABC, abstractmethod
from pathlib import Path
import polars as pl
import yaml

from etl.logger import get_logger

logger = get_logger(__name__)

DEFAULT_DATAPLATFORM_ROOT = "./dataplatform"


class Model(ABC):
    def __init__(
        self,
        name: str,
        layer: str,
        partitioning_columns: list[str] = [],
        dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__()
        self.name = name
        self.layer = layer
        self.partitioning_columns = partitioning_columns
        self._df: pl.DataFrame | None = None
        self._lf: pl.LazyFrame | None = None
        self.dataplatform_root = dataplatform_root
        self._dependencies: list[type] = []

    def configure_dependencies(self, dependencies: list[type]) -> None:
        for dep in dependencies:
            if not (
                isinstance(dep, type) and issubclass(dep, Model) and dep is not Model
            ):
                raise TypeError(f"{dep!r} is not a concrete subclass of Model")
        self._dependencies = list(dependencies)

    @abstractmethod
    def _build(self) -> pl.DataFrame | pl.LazyFrame:
        pass

    def build(self) -> None:
        logger.info("Building from source data model %s", self.name)
        result = self._build()
        if isinstance(result, pl.LazyFrame):
            self._lf = result
            logger.info(
                "%s/%s build plan ready (lazy/streaming)", self.layer, self.name
            )
        else:
            self._df = result
            logger.info(
                "%s/%s built: %d rows × %d cols — %.1f MB",
                self.layer,
                self.name,
                self._df.height,
                self._df.width,
                self._df.estimated_size("mb"),
            )

    @property
    def df(self) -> pl.DataFrame:
        if self._df is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.df accessed before build() or load_from_disk() was called."
            )
        return self._df

    @property
    def dependencies(self) -> list[type]:
        return self._dependencies

    def store(self):
        """Stores the dataframe as parquet under the appropriate data `layer` directory within
        a directory `model_name`. To set hive-partitioning provide the (ordered) list of column names
        to use for partitioning.

        Also includes a .yaml file with schema details of the generated dataframe"""

        layer_dir = Path(self.dataplatform_root) / self.layer / self.name
        layer_dir.mkdir(parents=True, exist_ok=True)

        if self._lf is not None:
            # Streaming path: sink_parquet executes the lazy plan in fixed-size
            # batches and writes directly to disk, so the full frame is never
            # held in memory at once.
            if self.partitioning_columns:
                self._lf.sink_parquet(
                    pl.PartitionBy(layer_dir, key=self.partitioning_columns)
                )
            else:
                self._lf.sink_parquet(layer_dir / f"{self.name}.parquet")

            schema_data = {
                "model": self.name,
                "layer": self.layer,
                "partitioned_by": self.partitioning_columns,
                "columns": [
                    {"name": col, "dtype": str(dtype)}
                    for col, dtype in self._lf.schema.items()
                ],
            }
        else:
            if self.partitioning_columns:
                self.df.write_parquet(layer_dir, partition_by=self.partitioning_columns)
            else:
                self.df.write_parquet(layer_dir / f"{self.name}.parquet")

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

        schema_path = layer_dir / f"{self.name}_schema.yaml"
        with open(schema_path, "w") as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)

        logger.info("Stored %s/%s", self.layer, self.name)

    def free(self):
        """Drops the in-memory DataFrame, releasing its Arrow buffer memory.

        CPython's reference counting will invoke Rust's Drop immediately when
        no other references exist. gc.collect() handles the rare case where a
        reference cycle would otherwise delay the release.

        After this call, accessing df will raise until build() or
        load_from_disk() is called again."""
        self._df = None
        gc.collect()

    def build_store_free(self):
        """Runs build(), store(), and free() in a dedicated subprocess.

        Arrow-backed allocators (jemalloc/mimalloc) hold freed pages in their
        own pool, so RSS stays high even after free(). A subprocess exit forces
        the OS to reclaim all memory unconditionally, bypassing the allocator."""

        def _run(model):
            model.build()
            model.store()
            model.free()

        proc = multiprocessing.Process(target=_run, args=(self,))
        proc.start()
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(
                f"{self.layer}/{self.name} subprocess exited with code {proc.exitcode}"
            )

    def lazy_load(self) -> pl.LazyFrame:
        layer_dir = Path(DEFAULT_DATAPLATFORM_ROOT) / self.layer / self.name
        if self.partitioning_columns:
            return pl.scan_parquet(
                str(layer_dir / "**" / "*.parquet"), hive_partitioning=True
            )
        else:
            return pl.scan_parquet(layer_dir / f"{self.name}.parquet")

    def load_from_disk(self) -> pl.DataFrame:
        """Instead of computing the dataset from sources, reads it from
        the current version on disk as it gets saved by self.store"""

        logger.info("Loading from disk data model %s", self.name)
        layer_dir = Path(self.dataplatform_root) / self.layer / self.name
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


# TODO tbh i dont understand how this works but it passes tests so gg
def build_execution_plan(models: list[Model]) -> list[Model]:
    """Returns the same Model instances reordered so every model's dependencies
    appear before it.

    The graph is built from classes: each model class is a node and its
    set_dependencies() entries are predecessors. Instances not present in the
    input list are ignored (their dependency edges are dropped). Raises
    RuntimeError on circular dependencies."""

    class_to_instance = {type(m): m for m in models}
    known = set(class_to_instance)
    graph = {type(m): {dep for dep in m.dependencies if dep in known} for m in models}

    try:
        order = list(graphlib.TopologicalSorter(graph).static_order())
    except graphlib.CycleError as exc:
        cycle = " -> ".join(cls.__name__ for cls in exc.args[1])
        raise RuntimeError(f"Circular dependency detected: {cycle}") from exc

    return [class_to_instance[cls] for cls in order]
