import json
from pathlib import Path
import polars as pl

from etl.logger import get_logger
from etl.transformation.model import Model, DEFAULT_DATAPLATFORM_ROOT

logger = get_logger(__name__)

_NAMESPACES = ("dei", "us-gaap")
_CHUNK_SIZE = 500
_SCHEMA = {
    "cik": pl.Int64,
    "namespace": pl.String,
    "indicator": pl.String,
    "label": pl.String,
    "description": pl.String,
}


def _extract_rows(file_path: Path) -> list[dict]:
    try:
        data = json.loads(file_path.read_bytes())
    except Exception as e:
        logger.warning("Could not read %s: %s", file_path.name, e)
        return []

    cik = data.get("cik")
    facts = data.get("facts", {})

    rows = []
    for ns in _NAMESPACES:
        for indicator, entry in facts.get(ns, {}).items():
            rows.append(
                {
                    "cik": cik,
                    "namespace": ns,
                    "indicator": indicator,
                    "label": entry.get("label"),
                    "description": entry.get("description"),
                }
            )
    return rows


def compute_from_source(sec_data_path: str | Path) -> pl.LazyFrame:
    sec_dir = Path(sec_data_path)
    json_files = sorted(sec_dir.glob("*.json"))
    logger.debug("Using source: %s", sec_dir)

    chunks = []
    for i in range(0, len(json_files), _CHUNK_SIZE):
        batch = json_files[i : i + _CHUNK_SIZE]
        rows = []
        for file_path in batch:
            rows.extend(_extract_rows(file_path))
        if rows:
            chunks.append(pl.from_dicts(rows, schema=_SCHEMA).lazy())

    if not chunks:
        return pl.DataFrame(
            schema={
                "namespace": pl.String,
                "indicator": pl.String,
                "label": pl.String,
                "description": pl.String,
                "cik_count": pl.UInt32,
            }
        ).lazy()

    return (
        pl.concat(chunks)
        .group_by(["namespace", "indicator", "label", "description"])
        .agg(pl.col("cik").n_unique().alias("cik_count"))
        .sort(["cik_count"], descending=[True])
    )


class SecIndicatorsSilver(Model):
    def __init__(
        self,
        sec_data_path: str | Path | None = None,
        dataplatform_root: str | Path = DEFAULT_DATAPLATFORM_ROOT,
    ) -> None:
        super().__init__(
            name="sec_indicators", layer="silver", dataplatform_root=dataplatform_root
        )
        self.sec_data_path = sec_data_path

    def _build(self) -> pl.LazyFrame:
        if self.sec_data_path is None:
            raise ValueError("sec_data_path is required to build SecIndicatorsSilver")
        return compute_from_source(self.sec_data_path)
