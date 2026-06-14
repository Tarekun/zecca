import json
from pathlib import Path

import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model

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
    except Exception:
        return [null_row]

    cik = data.get("cik")
    entity_name = data.get("entityName")
    shares = (
        data.get("facts", {})
        .get("dei", {})
        .get("EntityCommonStockSharesOutstanding", {})
        .get("units", {})
        .get("shares") or []
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


def compute_from_source(sec_data_path: str | Path) -> pl.DataFrame:
    sec_dir = Path(sec_data_path)
    json_files = sorted(sec_dir.glob("*.json"))
    logger.info("Processing %d SEC JSON files from %s", len(json_files), sec_dir)

    chunks = []
    for i in range(0, len(json_files), _CHUNK_SIZE):
        batch = json_files[i : i + _CHUNK_SIZE]
        rows = []
        for file_path in batch:
            rows.extend(_extract_rows(file_path))
        chunks.append(pl.from_dicts(rows, schema=_SCHEMA))

    df = pl.concat(chunks).with_columns(
        pl.col("end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("filed").str.to_date(format="%Y-%m-%d", strict=False),
    )

    logger.info(
        "Returning sec_company_facts: %d rows × %d cols — %.1f MB",
        df.height,
        df.width,
        df.estimated_size("mb"),
    )
    return df


class SecCompanyFactsSilver(Model):
    def __init__(self, sec_data_path: str | Path | None = None) -> None:
        super().__init__(name="sec_company_facts", layer="silver")
        self.sec_data_path = sec_data_path

    def _build(self) -> pl.DataFrame:
        if self.sec_data_path is None:
            raise ValueError("sec_data_path is required to build SecCompanyFactsSilver")
        return compute_from_source(self.sec_data_path)
