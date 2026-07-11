from pathlib import Path
import polars as pl

from etl.logger import get_logger
from etl.transformation.silver.sec_indicators import SecIndicatorsSilver
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT

logger = get_logger(__name__)


class SecIndicatorsGold(Model):
    def __init__(self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT):
        super().__init__(
            name="sec_indicators", layer="gold", dataplatform_root=dataplatform_root
        )
        self.configure_dependencies([SecIndicatorsSilver])

    def _build(self) -> pl.LazyFrame:
        logger.debug("Using source: SecIndicatorsSilver")
        return (
            SecIndicatorsSilver()
            .read_from_disk()
            .sort("cik_count", descending=True)
            .select(["cik_count", "namespace", "indicator", "label", "description"])
        )

    def store(self):
        layer_dir = Path(self.dataplatform_root) / self.layer / self.name
        layer_dir.mkdir(parents=True, exist_ok=True)
        export = self.df
        export.write_csv(layer_dir / f"{self.name}.csv")
        logger.info(
            "Stored %s/%s as CSV: %d rows × %d cols",
            self.layer,
            self.name,
            export.height,
            export.width,
        )
