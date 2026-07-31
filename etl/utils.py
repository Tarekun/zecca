from pathlib import Path
import polars as pl
from etl.logger import get_logger

logger = get_logger(__name__)


def upsert_df(
    df: pl.DataFrame,
    name: str,
    base_dir: str,
    key_columns: list[str],
    partition_columns: list[str] | None = None,
):
    """Writes `df` under `base_dir/name` as parquet using a merge strategy: rows
    already on disk are matched against `df` by `key_columns`, matches are
    replaced by `df`'s version, non-matching existing rows are kept as-is, and
    unmatched rows from `df` are inserted.

    If `partition_columns` is given, the merge is scoped independently to each
    partition and results are written hive-style (`col=value/...`). Those
    columns must already exist in `df` -- they are not computed here."""

    root = Path(base_dir) / name
    root.mkdir(parents=True, exist_ok=True)

    if not partition_columns:
        _upsert_at(df, root, key_columns)
        return

    for values, part_df in df.group_by(partition_columns):
        part_path = root
        for col, val in zip(partition_columns, values):
            part_path = part_path / f"{col}={val}"
        part_path.mkdir(parents=True, exist_ok=True)
        _upsert_at(part_df, part_path, key_columns)


def _upsert_at(new_df: pl.DataFrame, path: Path, key_columns: list[str]):
    """Merges `new_df` into whatever parquet file(s) already exist at `path`,
    keeping `new_df`'s row on any `key_columns` conflict, and overwrites `path`
    with the merged result."""

    existing_files = list(path.glob("*.parquet"))
    if existing_files:
        existing_df = pl.concat([pl.read_parquet(f) for f in existing_files])
        combined = pl.concat([existing_df, new_df], how="vertical_relaxed")
    else:
        combined = new_df
    combined = combined.unique(subset=key_columns, keep="last", maintain_order=True)

    for f in existing_files:
        f.unlink()
    combined.write_parquet(path / "data.parquet", compression="snappy")
