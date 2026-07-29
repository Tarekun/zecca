import polars as pl

from etl.logger import get_logger
from etl.transformation.silver.stocks_daily import StocksDailySilver
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import not_empty

logger = get_logger(__name__)


class VisibilityGraphIndicatorsGold(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT):
        super().__init__(
            name="visibility_graph_indicators",
            layer="gold",
            kind="view",
            quality_checks=[not_empty()],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        logger.debug("Using source: StocksDailySilver")
        return StocksDailySilver().read_from_disk()
