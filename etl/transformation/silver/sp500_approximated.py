from pathlib import Path

import polars as pl
import yaml

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.silver.stocks_daily import StocksDailySilver

logger = get_logger(__name__)


def compute_from_source() -> pl.LazyFrame:
    """Read stocks_daily and produce a per-date ranking of the top 600 stocks by
    float-adjusted market cap.

    ``float_adjusted_market_cap`` = ``estimated_float_shares`` × ``open``.  Only
    rows where both values are non-null and open is positive are eligible for
    ranking.  Stocks are ranked within each ``timeframe`` (rank 1 = largest
    float-adjusted market cap), and only the top 600 per date are kept.

    Returns:
        LazyFrame with columns:

        - ``timeframe``                 – trading date
        - ``symbol``                    – ticker symbol
        - ``open``                      – opening price on that date
        - ``estimated_float_shares``    – forward-filled float share count
        - ``float_adjusted_market_cap`` – estimated_float_shares × open
        - ``rank``                      – 1-based rank within the date (1 = largest)
    """
    logger.debug("Using source: StocksDailySilver")

    return (
        StocksDailySilver()
        .read_from_disk()
        .select(["timeframe", "symbol", "open", "estimated_float_shares"])
        .filter(
            pl.col("estimated_float_shares").is_not_null()
            & pl.col("open").is_not_null()
            & (pl.col("open") > 0)
        )
        .with_columns(
            (pl.col("estimated_float_shares") * pl.col("open")).alias("float_adjusted_market_cap")
        )
        .with_columns(
            pl.col("float_adjusted_market_cap")
            .rank(method="ordinal", descending=True)
            .over("timeframe")
            .alias("rank")
        )
        .filter(pl.col("rank") <= 600)
        .sort(["timeframe", "rank"])
    )


class Sp500ApproximatedSilver(Model):
    def __init__(self, dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="sp500_approximated", layer="silver", dataplatform_root=dataplatform_root
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source()

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
            .sort("rank")
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
