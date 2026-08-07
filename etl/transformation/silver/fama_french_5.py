from pathlib import Path
import polars as pl

from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT
from etl.transformation.quality_checks import (
    is_finite,
    no_gaps,
    not_empty,
    not_null,
    unique,
)

FACTOR_COLUMNS = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"]
_FILENAME_SUFFIX = "_5_factors_daily.csv"


def compute_from_source(french_library_dir: Path) -> pl.LazyFrame:
    """Reads every `<region>_5_factors_daily.csv` left by the french_library
    ingestion and unifies them into a single LazyFrame, tagging each row with
    the `region` its file came from.

    Values in the source CSVs are padded with whitespace to a fixed width, so
    every column but the date is read as a string and stripped before casting
    to float.
    """
    regions = []
    for csv_path in sorted(french_library_dir.glob(f"*{_FILENAME_SUFFIX}")):
        region = csv_path.name.removesuffix(_FILENAME_SUFFIX)
        lf = (
            pl.scan_csv(csv_path, infer_schema=False)
            .rename({"": "date"})
            .with_columns(
                pl.col("date").str.strip_chars().str.strptime(pl.Date, "%Y%m%d")
            )
            .with_columns(
                [pl.col(c).str.strip_chars().cast(pl.Float64) for c in FACTOR_COLUMNS]
            )
            .with_columns(pl.lit(region).alias("region"))
        )
        regions.append(lf)

    return pl.concat(regions)


class FamaFrench5Silver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="fama_french_5",
            layer="silver",
            partitioning_columns=["region"],
            quality_checks=[
                not_empty(),
                unique(["region", "date"]),
                is_finite(FACTOR_COLUMNS),
                not_null(["date", "region", *FACTOR_COLUMNS]),
                no_gaps("date", "region", skip_weekends=True),
            ],
            dataplatform_root=dataplatform_root,
        )

    def _build(self) -> pl.LazyFrame:
        return compute_from_source(
            Path(self.dataplatform_root) / "raw" / "french_library"
        )
