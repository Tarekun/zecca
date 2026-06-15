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
    "shares_outstanding_end": pl.String,
    "shares_outstanding_filed": pl.String,
    "shares_outstanding_fp": pl.String,
    "shares_outstanding": pl.Int64,
    "public_float_end": pl.String,
    "public_float_filed": pl.String,
    "non_affiliate_valuation": pl.Int128,
}
_CHUNK_SIZE = 500


def _extract_rows(file_path: Path) -> list[dict]:
    """Extract EntityCommonStockSharesOutstanding and EntityPublicFloat rows from one SEC JSON file.

    Always returns at least one row. If any key in the nested path is absent the
    metric-specific columns are null so no file is silently dropped.
    """

    null_row = {
        "cik": None,
        "entity_name": None,
        "source_file": file_path.name,
        "shares_outstanding_end": None,
        "shares_outstanding_filed": None,
        "shares_outstanding_fp": None,
        "shares_outstanding": None,
        "public_float_end": None,
        "public_float_filed": None,
        "non_affiliate_valuation": None,
    }
    try:
        data = json.loads(file_path.read_bytes())
    except Exception as e:
        logger.warning("Could not read %s: %s", file_path.name, e)
        return [null_row]

    cik = data.get("cik")
    entity_name = data.get("entityName")
    common = {"cik": cik, "entity_name": entity_name, "source_file": file_path.name}

    dei = data.get("facts", {}).get("dei", {})

    shares_entries = (
        dei.get("EntityCommonStockSharesOutstanding", {}).get("units", {}).get("shares") or []
    )
    float_entries = (
        dei.get("EntityPublicFloat", {}).get("units", {}).get("USD") or []
    )

    shares_rows = [
        {
            **common,
            "shares_outstanding_end": e.get("end"),
            "shares_outstanding_filed": e.get("filed"),
            "shares_outstanding_fp": e.get("fp"),
            "shares_outstanding": e.get("val"),
            "public_float_end": None,
            "public_float_filed": None,
            "non_affiliate_valuation": None,
        }
        for e in shares_entries
    ]

    float_rows = [
        {
            **common,
            "shares_outstanding_end": None,
            "shares_outstanding_filed": None,
            "shares_outstanding_fp": None,
            "shares_outstanding": None,
            "public_float_end": e.get("end"),
            "public_float_filed": e.get("filed"),
            "non_affiliate_valuation": e.get("val"),
        }
        for e in float_entries
    ]

    rows = shares_rows + float_rows
    if not rows:
        return [{**null_row, "cik": cik, "entity_name": entity_name}]

    return rows


def compute_from_source(sec_data_path: str | Path) -> pl.DataFrame:
    """Parse all SEC company facts JSON files under `sec_data_path` and return a
    flat DataFrame of EntityCommonStockSharesOutstanding and EntityPublicFloat entries.

    Each metric's entries are represented as separate rows; metric-specific columns
    are null on rows belonging to the other metric.

    Files are processed sequentially in chunks so only a small window of JSON
    data is held in memory at any time.

    Returns:
        Eager DataFrame with columns:

        - ``cik``                     – company CIK (integer)
        - ``entity_name``             – from entityName
        - ``source_file``             – originating filename
        - ``shares_outstanding_end``  – period end date for shares outstanding
        - ``shares_outstanding_filed``– filing date for shares outstanding
        - ``shares_outstanding_fp``   – fiscal period for shares outstanding
        - ``shares_outstanding``      – shares outstanding count
        - ``public_float_end``        – period end date for public float
        - ``public_float_filed``      – filing date for public float
        - ``non_affiliate_valuation`` – public float value in USD
    """

    sec_dir = Path(sec_data_path)
    json_files = sorted(sec_dir.glob("*.json"))
    logger.debug("Using source: %s", sec_dir)

    chunks = []
    for i in range(0, len(json_files), _CHUNK_SIZE):
        batch = json_files[i : i + _CHUNK_SIZE]
        rows = []
        for file_path in batch:
            rows.extend(_extract_rows(file_path))
        chunks.append(pl.from_dicts(rows, schema=_SCHEMA))

    df = pl.concat(chunks).with_columns(
        pl.col("shares_outstanding_end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("shares_outstanding_filed").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("public_float_end").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("public_float_filed").str.to_date(format="%Y-%m-%d", strict=False),
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
