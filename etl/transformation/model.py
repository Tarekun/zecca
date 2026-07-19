import ast
import gc
import graphlib
import inspect
import shutil
import textwrap
from abc import ABC, abstractmethod
from pathlib import Path
import polars as pl
from typing import Literal
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
        dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
        kind: Literal["table", "view"] = "table",
    ) -> None:
        super().__init__()
        self.name = name
        self.layer = layer
        self.partitioning_columns = partitioning_columns
        self._lf: pl.LazyFrame | None = None
        self.dataplatform_root = dataplatform_root
        self._configured_dependencies: list[type] | None = None
        self._discovered_dependencies: list[type] | None = None
        # TODO: currently models only support full refresh processing
        # this variable was defined anyway as a future proof flag for store FR processing
        self.strategy: Literal["fullrefresh"] = "fullrefresh"
        self.kind = kind

    def configure_dependencies(self, dependencies: list[type]) -> None:
        """Explicitly overrides dependency auto-discovery (see the `dependencies`
        property) with a fixed list. Use this when a dependency is only reached
        through a code path that static inspection of `_build` can't see, e.g.
        one guarded by a runtime condition."""
        for dep in dependencies:
            if not (
                isinstance(dep, type) and issubclass(dep, Model) and dep is not Model
            ):
                raise TypeError(f"{dep!r} is not a concrete subclass of Model")
        self._configured_dependencies = list(dependencies)

    @property
    def id(self) -> str:
        return f"{self.layer}.{self.name}"

    @abstractmethod
    def _build(self) -> pl.LazyFrame:
        pass

    def build(self) -> pl.LazyFrame:
        logger.info("Building from source data model %s", self.id)
        self._lf = self._build()
        try:
            self._lf.collect_schema()
        except pl.exceptions.PolarsError:
            logger.exception("%s build plan is invalid", self.id)
            raise
        logger.info("%s build plan ready and validated", self.id)
        return self._lf

    @property
    def df(self) -> pl.DataFrame:
        """Materializes the current lazy plan into an in-memory DataFrame.

        This is the only place a full DataFrame is instantiated; the result
        is returned directly and never cached on the instance."""
        if self._lf is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.df accessed before build() or read_from_disk() was called."
            )
        return self._lf.collect()

    @property
    def dependencies(self) -> list[type]:
        """The other Model subclasses this model needs built first.

        Defaults to statically discovering them from `_build`'s own source
        (see `_discover_dependencies`); call `configure_dependencies` to
        override with an explicit list instead."""
        if self._configured_dependencies is not None:
            return self._configured_dependencies

        if self._discovered_dependencies is None:
            self._discovered_dependencies = self._discover_dependencies()
        return self._discovered_dependencies

    def _discover_dependencies(self) -> list[type]:
        """Statically walks the AST of this model's `_build` implementation,
        recursing into any plain function it calls (transitively, within the
        `etl` package), looking for references to other `Model` subclasses.

        This is a pure function of `type(self)`'s own source code: the only
        state involved is the local `discovered`/`visited` sets built up
        during this single call, so there is nothing shared across instances
        or calls."""
        discovered: set[type] = set()
        visited_functions: set[object] = set()
        _collect_model_refs(type(self)._build, discovered, visited_functions)
        discovered.discard(type(self))
        return sorted(discovered, key=lambda cls: cls.__qualname__)

    def store(self):
        """Stores the lazy plan as parquet under the appropriate data `layer` directory within
        a directory `model_name`. To set hive-partitioning provide the (ordered) list of column names
        to use for partitioning.

        Also includes a .yaml file with schema details of the generated dataset"""

        if self._lf is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.store() called before build() or read_from_disk()."
            )

        model_dir = Path(self.dataplatform_root) / self.layer / self.name
        if model_dir.exists() and self.strategy == "fullrefresh":
            # sink_parquet only adds files so we need to clean previous
            shutil.rmtree(model_dir)
        model_dir.mkdir(parents=True, exist_ok=True)

        view_skip_log = (
            f"{self.layer}.{self.name} is of VIEW kind, skipping disk storage"
        )
        if self.partitioning_columns:
            row_count = (
                pl.scan_parquet(
                    str(model_dir / "**" / "*.parquet"), hive_partitioning=True
                )
                .select(pl.len())
                .collect()
                .item()
            )
            if self.kind == "view":
                logger.info(view_skip_log)
            else:
                self._lf.sink_parquet(
                    pl.PartitionBy(model_dir, key=self.partitioning_columns)
                )

        else:
            output_path = model_dir / f"{self.name}.parquet"
            row_count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
            if self.kind == "view":
                logger.info(view_skip_log)
            else:
                self._lf.sink_parquet(output_path)

        schema_data = {
            "model": self.name,
            "layer": self.layer,
            "partitioned_by": self.partitioning_columns,
            "row_count": row_count,
            "columns": [
                {"name": col, "dtype": str(dtype)}
                for col, dtype in self._lf.schema.items()
            ],
        }
        schema_path = model_dir / f"{self.name}_schema.yaml"
        with open(schema_path, "w") as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)

        logger.info("Stored %s/%s", self.layer, self.name)

    def free(self):
        """Drops the in-memory lazy plan, releasing any buffers it holds.

        CPython's reference counting will invoke Rust's Drop immediately when
        no other references exist. gc.collect() handles the rare case where a
        reference cycle would otherwise delay the release.

        After this call, accessing df will raise until build() or
        read_from_disk() is called again."""
        self._lf = None
        gc.collect()

    def build_store_free(self):
        """Runs build() and store().

        Since the pipeline only ever builds a lazy plan and sink_parquet
        streams it to disk in fixed-size batches, no full frame is ever
        materialized in memory here, so there's nothing for a dedicated
        subprocess to reclaim."""
        self.build()
        self.store()

    def read_from_disk(self) -> pl.LazyFrame:
        """Instead of computing the dataset from sources, scans it from
        the current version on disk as it gets saved by self.store"""

        if self.kind == "view":
            logger.info(
                f"{self.layer}.{self.name} is of VIEW kind and cannot be read from disk, defaulting to .build() call"
            )
            return self.build()

        else:
            layer_dir = Path(self.dataplatform_root) / self.layer / self.name
            if self.partitioning_columns:
                glob_path = str(layer_dir / "**" / "*.parquet")
                self._lf = pl.scan_parquet(glob_path, hive_partitioning=True)
            else:
                self._lf = pl.scan_parquet(layer_dir / f"{self.name}.parquet")
            return self._lf


def _resolve_ast_node(node: ast.AST, scope: dict):
    """Resolves a Name/Attribute AST node to the runtime object it refers to,
    using `scope` (a function's __globals__) to look up root names. Returns
    None for anything it can't resolve (locals, calls, literals, ...)."""

    if isinstance(node, ast.Name):
        return scope.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _resolve_ast_node(node.value, scope)
        return getattr(base, node.attr, None) if base is not None else None
    return None


def _collect_model_refs(func, discovered: set[type], visited_functions: set) -> None:
    """Recursively scans `func`'s source for references to Model subclasses,
    following calls into any other plain function defined within the `etl`
    package (so indirection through helper functions like the codebase's
    `compute_from_source` is still followed)"""

    if func in visited_functions:
        return
    visited_functions.add(func)

    try:
        source = textwrap.dedent(inspect.getsource(func))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return

    scope = getattr(func, "__globals__", {})
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Name, ast.Attribute)):
            continue
        obj = _resolve_ast_node(node, scope)
        if isinstance(obj, type) and issubclass(obj, Model) and obj is not Model:
            discovered.add(obj)
        elif inspect.isfunction(obj) and (obj.__module__ or "").startswith("etl"):
            _collect_model_refs(obj, discovered, visited_functions)


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
