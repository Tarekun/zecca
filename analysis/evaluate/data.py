"""Market data for evaluations: a parquet snapshot of `gold.stocks_ml_ready`.

`gold.stocks_ml_ready` is a view (recomputed on every read), so the CLI
snapshots it once and every evaluation scans only the columns and dates it
needs. The snapshot is rebuilt with `--refresh-data` (e.g. after the daily
pipeline ran)."""

import contextlib
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from analysis.evaluate.config import EvaluationConfig
from analysis.framework import DATE_COL, SYMBOL_COL, load_risk_free

BASE_COLUMNS = [DATE_COL, SYMBOL_COL, "open", "high", "low", "close", "volume"]


class DataError(ValueError):
    pass


def _meta_path(cache: Path) -> Path:
    return cache.with_name(cache.stem + ".meta.json")


def build_cache(config: EvaluationConfig) -> dict[str, Any]:
    from etl.transformation.gold import StocksMlReadyGold

    # the etl pipeline logs to stdout, which is reserved for the JSON report
    with contextlib.redirect_stdout(sys.stderr):
        lf = StocksMlReadyGold(
            dataplatform_root=str(config.dataplatform_root)
        ).read_from_disk()
        df = lf.with_columns(
            pl.col(name).cast(pl.Float64)
            for name, dtype in lf.collect_schema().items()
            if dtype == pl.Int128  # not supported by every parquet reader
        ).collect()

    config.data_cache.parent.mkdir(parents=True, exist_ok=True)
    tmp = config.data_cache.with_suffix(".tmp")
    df.write_parquet(tmp)
    tmp.replace(config.data_cache)
    meta = {
        "source": "gold.stocks_ml_ready",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "rows": df.height,
        "symbols": df[SYMBOL_COL].n_unique(),
        "first_day": df[DATE_COL].min().isoformat(),  # type: ignore[union-attr]
        "last_day": df[DATE_COL].max().isoformat(),  # type: ignore[union-attr]
    }
    _meta_path(config.data_cache).write_text(json.dumps(meta, indent=2))
    return meta


def cache_meta(config: EvaluationConfig, refresh: bool = False) -> dict[str, Any]:
    if refresh or not config.data_cache.exists():
        return build_cache(config)
    meta_file = _meta_path(config.data_cache)
    return json.loads(meta_file.read_text()) if meta_file.exists() else {}


def load_market(
    config: EvaluationConfig, columns: list[str], end: date | None
) -> pl.DataFrame:
    """Columns `BASE_COLUMNS + columns` for days before `end` (exclusive)."""
    lf = pl.scan_parquet(config.data_cache)
    available = lf.collect_schema().names()
    wanted = list(dict.fromkeys(BASE_COLUMNS + columns))
    missing = [c for c in wanted if c not in available]
    if missing:
        raise DataError(f"Unknown data column(s) {missing}; available: {available}")
    if end is not None:
        lf = lf.filter(pl.col(DATE_COL) < end)
    df = lf.select(wanted).collect()
    if df.is_empty():
        raise DataError(f"No market data before {end}")
    return df


def load_rf(config: EvaluationConfig) -> pl.DataFrame | None:
    if config.risk_free_region is None:
        return None
    with contextlib.redirect_stdout(sys.stderr):
        return load_risk_free(config.risk_free_region, str(config.dataplatform_root))
