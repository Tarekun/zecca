import io
import os
import zipfile

import requests

from etl.ingestion.source import DEFAULT_DATAPLATFORM_ROOT, DictLike
from etl.logger import get_logger

logger = get_logger(__name__)


# TODO in caso di IP ban mettere qualcosa di legit qua
USER_AGENT = "Moe Lester plsdontba.nmeagain@gmail.com"


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


class SecTickers(DictLike):
    """The SEC's CIK/ticker/company-name mapping: a single JSON file
    (`company_tickers.json`) covering every registrant."""

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_tickers", **kwargs)

    def load(self, **kwargs):
        headers = {"User-Agent": USER_AGENT}  # scegliere una mail da sostituire con ***
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json", headers=headers
        )
        r.raise_for_status()
        return r.json()


class SecCompanyFacts(DictLike):
    """SEC XBRL "company facts" for every registrant, one JSON file per CIK,
    extracted from the SEC's bulk `companyfacts.zip` download -- a
    collection source, indexed by CIK (e.g. `sec["CIK0000320193"]`)."""

    _URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_facts", **kwargs)

    def load(self, **kwargs) -> None:
        """Downloads and extracts the zip directly into this source's
        directory -- unlike the generic DictLike case, the per-CIK files
        are written as a side effect of the download itself rather than
        held in memory as one big payload, so `_persist()` is a no-op."""
        download_and_unzip(self._URL, str(self.root_dir), USER_AGENT)

    def _persist(self, data) -> None:
        pass
