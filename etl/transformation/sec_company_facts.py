import json
from pathlib import Path
import polars as pl

from etl.logger import get_logger

logger = get_logger(__name__)

_SCHEMA = {
    "cik": pl.Int64,
    "entity_name": pl.String,
    "source_file": pl.String,
    "end": pl.String,
    "filed": pl.String,
    "fp": pl.String,
    "val": pl.Int64,
}

_CHUNK_SIZE = 500


def _extract_rows(file_path: Path) -> list[dict]:
    """Extract EntityCommonStockSharesOutstanding rows from one SEC JSON file.

    Always returns at least one row. If any key in the nested path is absent the
    shares-specific columns are null so no file is silently dropped.
    """
    null_row = {
        "cik": None,
        "entity_name": None,
        "source_file": file_path.name,
        "end": None,
        "filed": None,
        "fp": None,
        "val": None,
    }

    try:
        data = json.loads(file_path.read_bytes())
    except Exception as e:
        logger.warning("Could not read %s: %s", file_path.name, e)
        return [null_row]

    cik = data.get("cik")
    entity_name = data.get("entityName")

    shares = (
        data.get("facts", {})
        .get("dei", {})
        .get("EntityCommonStockSharesOutstanding", {})
        .get("units", {})
        .get("shares")
        or []
    )

    if not shares:
        return [{**null_row, "cik": cik, "entity_name": entity_name}]

    return [
        {
            "cik": cik,
            "entity_name": entity_name,
            "source_file": file_path.name,
            "end": entry.get("end"),
            "filed": entry.get("filed"),
            "fp": entry.get("fp"),
            "val": entry.get("val"),
        }
        for entry in shares
    ]


def compute_sec_company_facts(sec_data_path: str | Path) -> pl.DataFrame:
    """Parse all SEC company facts JSON files under `sec_data_path` and return a
    flat DataFrame of EntityCommonStockSharesOutstanding entries.

    Files are processed sequentially in chunks so only a small window of JSON
    data is held in memory at any time.

    Returns:
        Eager DataFrame with columns:

        - ``cik``         – company CIK (integer)
        - ``entity_name`` – from entityName
        - ``source_file`` – originating filename
        - ``end``         – period end date
        - ``filed``       – filing date
        - ``fp``          – fiscal period (Q1/Q2/Q3/Q4/FY …)
        - ``val``         – shares outstanding
    """
    sec_dir = Path(sec_data_path)
    json_files = sorted(sec_dir.glob("*.json"))
    logger.info("Processing %d SEC JSON files from %s", len(json_files), sec_dir)

    chunks: list[pl.DataFrame] = []
    for i in range(0, len(json_files), _CHUNK_SIZE):
        batch = json_files[i : i + _CHUNK_SIZE]
        rows: list[dict] = []
        for file_path in batch:
            rows.extend(_extract_rows(file_path))
        chunks.append(pl.from_dicts(rows, schema=_SCHEMA))
        logger.info(
            "Processed files %d–%d / %d",
            i + 1,
            min(i + _CHUNK_SIZE, len(json_files)),
            len(json_files),
        )

    df = pl.concat(chunks).with_columns(
        pl.col("end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("filed").str.to_date(format="%Y-%m-%d", strict=False),
    )

    logger.info(
        "Returning shares_outstanding: %d rows × %d cols — %.1f MB",
        df.height,
        df.width,
        df.estimated_size("mb"),
    )
    return df
