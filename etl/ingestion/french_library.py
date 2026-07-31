import os

from etl.config import Config
from etl.ingestion.sec import download_and_unzip
from etl.logger import get_logger

logger = get_logger(__name__)

USER_AGENT = "Mozilla/5.0"

LEADING_ROWS_TO_PRUNE = 6
FRENCH_LIBRARY_BASE_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp"


def _ingest_ff5(config: Config, region_file_stem: str):
    extracted_name = f"{region_file_stem}_5_Factors_Daily.csv"
    url = f"{FRENCH_LIBRARY_BASE_URL}/{extracted_name.replace('.csv', '_CSV.zip')}"

    dest_dir = os.path.join(config.ingestion_dir, "french_library")
    dest_file = os.path.join(dest_dir, extracted_name.lower())

    logger.info(f"Starting download and extraction of {extracted_name}...")
    download_and_unzip(url, dest_dir, USER_AGENT)

    extracted_file = os.path.join(dest_dir, extracted_name)
    with open(extracted_file, "r") as f:
        lines = f.readlines()

    with open(dest_file, "w") as f:
        f.writelines(lines[LEADING_ROWS_TO_PRUNE:])

    os.remove(extracted_file)

    logger.info(f"Saved {extracted_name} to {dest_file}")


def ingest_ff5_north_america(config: Config):
    _ingest_ff5(config, "North_America")


def ingest_ff5_europe(config: Config):
    _ingest_ff5(config, "Europe")


def ingest_ff5_japan(config: Config):
    _ingest_ff5(config, "Japan")


def ingest_ff5_asia_pacific(config: Config):
    _ingest_ff5(config, "Asia_Pacific_ex_Japan")


def download_ff5_factors_daily(config: Config):
    ingest_ff5_north_america(config)
    ingest_ff5_europe(config)
    ingest_ff5_japan(config)
    ingest_ff5_asia_pacific(config)
