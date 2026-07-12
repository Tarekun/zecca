import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)

logger = get_logger(__name__)


class SymbolEmbeddingsSilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="symbol_embeddings",
            layer="silver",
            partitioning_columns=["not_before"],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        # TODO: should i just move the implementation here and leave the notebook as an experiment
        raise NotImplementedError(
            "Data model silver.symbol_embeddings must be build by executing its corresponding notebook"
        )
