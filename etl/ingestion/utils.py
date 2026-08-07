import io
import os
import zipfile

import requests

from etl.logger import get_logger

logger = get_logger(__name__)


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
