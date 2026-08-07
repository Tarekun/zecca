from typing import Literal

from etl.ingestion.sec import download_and_unzip
from etl.ingestion.source import TableLike
from etl.logger import get_logger

logger = get_logger(__name__)

Region = Literal["north_america", "europe", "japan", "asia_pacific_ex_japan"]

USER_AGENT = "Mozilla/5.0"
LEADING_ROWS_TO_PRUNE = 6
FRENCH_LIBRARY_BASE_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"
REGION_FILE_STEMS: dict[Region, str] = {
    "north_america": "North_America",
    "europe": "Europe",
    "japan": "Japan",
    "asia_pacific_ex_japan": "Asia_Pacific_ex_Japan",
}


class FrenchLibrary(TableLike):
    """Fama-French 5-factor daily returns from Ken French's data library, one
    region per instance.

    The library only publishes a zipped CSV with a few descriptive header
    rows prepended, so `load()` downloads, extracts and prunes it directly
    into this source's directory. The CSV is
    already where downstream models expect it once `load()` returns, so
    `_persist()` is a no-op.
    """

    def __init__(self, region: Region, **kwargs):
        super().__init__(
            name=f"french_library_{region}",
            key_columns=["date"],
            format="csv",
            **kwargs,
        )
        self.region: Region = region

    def load(self, **kwargs):
        region_file_stem = REGION_FILE_STEMS[self.region]
        extracted_name = f"{region_file_stem}_5_Factors_Daily.csv"
        extracted_file = self.root_dir / extracted_name
        url = f"{FRENCH_LIBRARY_BASE_URL}/{extracted_name.replace('.csv', '_CSV.zip')}"
        dest_file = self.root_dir / f"{region_file_stem.lower()}_5_factors_daily.csv"

        logger.info(f"Starting download and extraction of {extracted_name}...")
        download_and_unzip(url, str(self.root_dir), USER_AGENT)

        with open(extracted_file, "r") as f:
            lines = f.readlines()
        with open(dest_file, "w") as f:
            f.writelines(lines[LEADING_ROWS_TO_PRUNE:])
        extracted_file.unlink()

        logger.info(f"Saved {extracted_name} to {dest_file}")

    def _persist(self, data) -> None:
        pass
