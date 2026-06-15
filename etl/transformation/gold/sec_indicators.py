from pathlib import Path

import polars as pl

from etl.logger import get_logger
from etl.transformation.silver.sec_indicators import SecIndicatorsSilver
from etl.transformation.model import Model, DATAPLATFORM_ROOT

logger = get_logger(__name__)


class SecIndicatorsGold(Model):
    def __init__(self):
        super().__init__(name="sec_indicators", layer="gold")

    def _build(self) -> pl.DataFrame:
        logger.debug("Using source: SecIndicatorsSilver")
        return (
            SecIndicatorsSilver()
            .load_from_disk()
            .sort("cik_count", descending=True)
            .select(["cik_count", "namespace", "indicator", "label", "description"])
        )

    def store(self):
        layer_dir = Path(DATAPLATFORM_ROOT) / self.layer / self.name
        layer_dir.mkdir(parents=True, exist_ok=True)
        self.df.write_csv(layer_dir / f"{self.name}.csv")
        logger.info(
            "Stored %s/%s as CSV: %d rows × %d cols",
            self.layer,
            self.name,
            self.df.height,
            self.df.width,
        )
