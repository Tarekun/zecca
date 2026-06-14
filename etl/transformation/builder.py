import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

from etl.logger import get_logger
from etl.transformation.candles_daily import compute_candles_daily
from etl.transformation.sec_company_facts import compute_sec_company_facts
from etl.transformation.sec_company_facts_padded import compute_sec_company_facts_padded

logger = get_logger(__name__)

DATAPLATFORM_ROOT = "./dataplatform"


def _store_dataframe(
    df: pl.DataFrame, model_name: str, layer: str, partitioning_columns: list[str] = []
):
    """Stores the dataframe as parquet under the appropriate data `layer` directory within
    a directory `model_name`. To set hive-partitioning provide the (ordered) list of column names
    to use for partitioning.

    Also includes a .yaml file with schema details of the generated dataframe"""

    layer_dir = Path(DATAPLATFORM_ROOT) / layer / model_name
    layer_dir.mkdir(parents=True, exist_ok=True)

    if partitioning_columns:
        df.write_parquet(layer_dir, partition_by=partitioning_columns)
    else:
        df.write_parquet(layer_dir / f"{model_name}.parquet")

    # create a schema file with information about the stored dataset
    schema_path = layer_dir / f"{model_name}_schema.yaml"
    schema_data = {
        "model": model_name,
        "layer": layer,
        "partitioned_by": partitioning_columns,
        "row_count": df.height,
        "columns": [
            {"name": col, "dtype": str(dtype)}
            for col, dtype in zip(df.columns, df.dtypes)
        ],
    }
    with open(schema_path, "w") as f:
        yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)

    logger.info(
        "Stored %s/%s: %d rows × %d cols", layer, model_name, df.height, df.width
    )


def _backup_transformed():
    """Backs up the data platform layers that currently exist before transformations"""

    root = Path(DATAPLATFORM_ROOT)
    today_str = datetime.now().strftime("%Y%m%d")
    backup_dir = root / "backups" / today_str
    backup_dir.mkdir(parents=True, exist_ok=True)

    for layer in ("silver", "gold"):
        src = root / layer
        if src.exists():
            dst = backup_dir / layer
            shutil.copytree(src, dst, dirs_exist_ok=True)
            logger.info("Backed up %s → %s", src, dst)

    cutoff = datetime.now() - timedelta(days=30)
    for entry in (root / "backups").iterdir():
        if not entry.is_dir():
            continue
        try:
            entry_date = datetime.strptime(entry.name, "%Y%m%d")
        except ValueError:
            continue
        if entry_date < cutoff:
            shutil.rmtree(entry)
            logger.info("Removed stale backup: %s", entry)


def build_silver():
    """Build all silver layer models"""

    candles_daily = compute_candles_daily(f"{DATAPLATFORM_ROOT}/raw/")
    _store_dataframe(
        candles_daily, "candles_daily", "silver", partitioning_columns=["year", "month"]
    )

    sec_company_facts = compute_sec_company_facts(f"{DATAPLATFORM_ROOT}/raw/sec/")
    _store_dataframe(sec_company_facts, "sec_company_facts", "silver")

    sec_company_facts_padded = compute_sec_company_facts_padded(DATAPLATFORM_ROOT)
    _store_dataframe(sec_company_facts_padded, "sec_company_facts_padded", "silver")


def build_gold():
    """Build all gold layer models, assuming silver models have been processed"""

    silver_glob = str(
        Path(DATAPLATFORM_ROOT)
        / "silver"
        / "candles_daily"
        / "year=*"
        / "month=*"
        / "*.parquet"
    )
    stocks_daily = pl.scan_parquet(silver_glob, hive_partitioning=True).collect()
    _store_dataframe(
        stocks_daily, "stocks_daily", "gold", partitioning_columns=["year", "month"]
    )


def build_everything():
    try:
        _backup_transformed()
        build_silver()
        build_gold()
    except Exception as e:
        logger.error("Build failed: %s", e)
        raise e
