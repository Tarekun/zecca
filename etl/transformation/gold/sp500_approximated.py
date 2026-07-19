from pathlib import Path

import polars as pl
import yaml

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.stocks_rankings import StocksRankingsSilver

logger = get_logger(__name__)

_TOP_N = 600


class Sp500ApproximatedGold(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="sp500_approximated", layer="gold", dataplatform_root=dataplatform_root
        )

    def _build(self) -> pl.LazyFrame:
        logger.debug("Using source: StocksRankingsSilver")
        return (
            StocksRankingsSilver()
            .read_from_disk()
            .filter(pl.col("float_adjusted_market_cap_rank") <= _TOP_N)
            .sort(["timeframe", "float_adjusted_market_cap_rank"])
        )

    def store(self):
        """Write only the latest date's snapshot as a CSV, sorted by rank ascending.

        Only the latest date's rows are ever collected into memory — the
        rest of the lazy plan (every historical date's top-600 ranking) is
        never materialized."""
        if self._lf is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.store() called before build() or read_from_disk()."
            )

        layer_dir = Path(self.dataplatform_root) / self.layer / self.name
        layer_dir.mkdir(parents=True, exist_ok=True)

        latest_date = self._lf.select(pl.col("timeframe").max()).collect().item()
        export = (
            self._lf.filter(pl.col("timeframe") == latest_date)
            .drop("timeframe")
            .sort("float_adjusted_market_cap_rank")
            .collect()
        )

        csv_path = layer_dir / f"{self.name}.csv"
        export.write_csv(csv_path)

        schema_path = layer_dir / f"{self.name}_schema.yaml"
        schema_data = {
            "model": self.name,
            "layer": self.layer,
            "exported_date": str(latest_date),
            "row_count": export.height,
            "columns": [
                {"name": col, "dtype": str(dtype)}
                for col, dtype in zip(export.columns, export.dtypes)
            ],
        }
        with open(schema_path, "w") as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "Stored %s/%s: %d rows (date=%s) → %s",
            self.layer,
            self.name,
            export.height,
            latest_date,
            csv_path,
        )
