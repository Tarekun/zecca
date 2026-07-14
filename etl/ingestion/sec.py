import requests
import zipfile
import io
import os

from etl.logger import get_logger
from etl.config import Config

logger = get_logger(__name__)


# TODO in caso di IP ban mettere qualcosa di legit qua
USER_AGENT = "Zecca s@a.net"


def download_and_unzip(url: str, dest_path: str, user_agent: str) -> None:
    os.makedirs(dest_path, exist_ok=True)

    headers = {"User-Agent": user_agent}

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise

    content_length = response.headers.get("Content-Length")
    total_size = int(content_length) if content_length else None
    if total_size:
        logger.info(f"File size: {total_size / (1024 * 1024):.1f} MB")

    zip_bytes = io.BytesIO()
    downloaded = 0
    last_logged_pct = 0
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            zip_bytes.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = downloaded / total_size * 100
                if pct - last_logged_pct >= 10:
                    last_logged_pct = (pct // 10) * 10
                    logger.debug(
                        f"{last_logged_pct:.0f}% — {downloaded / (1024 * 1024):.1f} MBi"
                    )

    logger.info(f"Download complete: {downloaded / (1024 * 1024):.1f} MBi")

    zip_bytes.seek(0)

    try:
        with zipfile.ZipFile(zip_bytes) as z:
            z.extractall(dest_path)
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid zip file: {e}")
        raise

    logger.info(f"Extraction completed: {dest_path}")


def download_company_facts(config: Config) -> None:
    url = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
    logger.info("Starting download and extraction of company facts...")

    download_and_unzip(url, f"{config.ingestion_dir}/sec", USER_AGENT)


def download_sec_tickers(config: Config) -> None:
    os.makedirs(config.ingestion_dir, exist_ok=True)
    headers = {"User-Agent": USER_AGENT}  # scegliere una mail da sostituire con ***
    r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers)
    r.raise_for_status()
    data = r.content

    dest_file = os.path.join(config.ingestion_dir, "company_tickers.json")
    with open(dest_file, "wb") as f:
        f.write(data)

    logger.info(f"Saved company tickers to {dest_file}")
