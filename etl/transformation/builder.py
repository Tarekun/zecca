import shutil
from datetime import datetime, timedelta
from pathlib import Path

from etl.config import Config
from etl.logger import get_logger
from etl.transformation.model import (
    DEFAULT_DATAPLATFORM_ROOT,
    Model,
    build_execution_plan,
)
from etl.transformation.silver import *
from etl.transformation.gold.stocks_daily import StocksDailyGold

logger = get_logger(__name__)


def _backup_transformed():
    """Backs up the data platform layers that currently exist before transformations"""

    root = Path(DEFAULT_DATAPLATFORM_ROOT)
    today_str = datetime.now().strftime("%Y%m%d")
    backup_dir = root / "backups" / today_str
    backup_dir.mkdir(parents=True, exist_ok=True)

    for layer in ("silver", "gold"):
        src = root / layer
        if src.exists():
            dst = backup_dir / layer
            # TODO: if incremental processing is ever supported switch this back to a copy
            # shutil.copytree(src, dst, dirs_exist_ok=True)
            shutil.move(src, dst)
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


def build_silver(config: Config):
    models: list[Model] = [
        CompanyTickersSilver(f"{DEFAULT_DATAPLATFORM_ROOT}/raw/"),
        CandlesDailySilver(f"{DEFAULT_DATAPLATFORM_ROOT}/raw/"),
        SecCompanyFactsSilver(f"{DEFAULT_DATAPLATFORM_ROOT}/raw/sec/"),
        SecCompanyFactsPaddedSilver(),
        StocksDailySilver(),
        SymbolEmbeddingsSilver(),
    ]
    if config.selected is not None:
        models = [m for m in models if m.name in config.selected]

    for model in build_execution_plan(models):
        model.build_store_free()


def build_gold(config: Config):
    models: list[Model] = [StocksDailyGold()]
    if config.selected is not None:
        models = [m for m in models if m.name in config.selected]

    for model in build_execution_plan(models):
        model.build_store_free()


def build_everything(config: Config):
    try:
        _backup_transformed()
        build_silver(config)
        build_gold(config)
    except Exception as e:
        logger.error("Build failed: %s", e)
        raise e
