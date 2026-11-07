import gzip
import io
import re
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

import polars as pl
import requests

from etl.ingestion.source import DEFAULT_DATAPLATFORM_ROOT, DictLike, TableLike
from etl.ingestion.utils import download_and_unzip
from etl.logger import get_logger

logger = get_logger(__name__)


# TODO in caso di IP ban mettere qualcosa di legit qua
USER_AGENT = "Moe Lester plsdontba.nmeagain@gmail.com"

# SEC's fair-access policy allows at most 10 requests/second
_SEC_REQUEST_INTERVAL_SECONDS = 0.2
_SEC_MAX_RETRIES = 5


def _sec_get(url: str, stream: bool = False) -> requests.Response:
    """GET against sec.gov with the declared User-Agent, throttled to stay
    well under the fair-access rate limit and retried with exponential
    backoff on rate limiting (429/403) and server errors."""
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(_SEC_MAX_RETRIES):
        time.sleep(_SEC_REQUEST_INTERVAL_SECONDS)
        try:
            response = requests.get(url, headers=headers, stream=stream, timeout=120)
        except requests.exceptions.RequestException as exc:
            logger.warning("GET %s failed (%s), retrying", url, exc)
        else:
            if response.status_code not in (403, 429) and response.status_code < 500:
                response.raise_for_status()
                return response
            logger.warning("GET %s returned %s, retrying", url, response.status_code)
        time.sleep(2**attempt * 5)
    raise RuntimeError(f"GET {url} failed after {_SEC_MAX_RETRIES} attempts")


class SecTickers(DictLike):
    """The SEC's CIK/ticker/company-name mapping: a single JSON file
    (`company_tickers.json`) covering every *current* registrant."""

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_tickers", **kwargs)

    def load(self, **kwargs):
        return _sec_get("https://www.sec.gov/files/company_tickers.json").json()


class SecTickersExchange(DictLike):
    """Same mapping as `SecTickers` plus the exchange each ticker currently
    trades on (`company_tickers_exchange.json`): a JSON object with
    `fields` (cik, name, ticker, exchange) and `data` (one list per ticker).

    Used instead of the much larger bulk `submissions.zip`: for current
    registrants the two carry the same tickers/exchanges, and for inactive
    ones `submissions.zip` has no ticker at all (verified on Twitter and
    Lehman Brothers), while company names over time are already in the
    full-index."""

    def __init__(self, **kwargs) -> None:
        super().__init__(name="company_tickers_exchange", **kwargs)

    def load(self, **kwargs):
        return _sec_get(
            "https://www.sec.gov/files/company_tickers_exchange.json"
        ).json()


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


# --- EDGAR full-index ---------------------------------------------------------

FULL_INDEX_FIRST_YEAR = 1993
# a quarter downloaded at least this long after it ended is considered final:
# EDGAR keeps regenerating the index files nightly, but past quarters no
# longer change in content
_QUARTER_SETTLE_DAYS = 7

_FULL_INDEX_SCHEMA = {
    "cik": pl.Int64,
    "company_name": pl.String,
    "form_type": pl.String,
    "date_filed": pl.Date,
    "accession_number": pl.String,
    "filename": pl.String,
}


def _quarter_of(day: date) -> tuple[int, int]:
    return day.year, (day.month - 1) // 3 + 1


def _quarter_end(year: int, quarter: int) -> date:
    if quarter == 4:
        return date(year, 12, 31)
    return date(year, 3 * quarter + 1, 1) - timedelta(days=1)


def parse_master_index(text: str) -> pl.DataFrame:
    """Parses an EDGAR `master.idx` file (`CIK|Company Name|Form Type|Date
    Filed|Filename`, after a free-text header closed by a dashed line) into
    a DataFrame with `_FULL_INDEX_SCHEMA`.

    Company names may themselves contain `|`, so fields are taken from both
    ends of each line and whatever is left in the middle is the name."""
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.startswith("-----")) + 1
    except StopIteration:
        raise ValueError("not an EDGAR master index: no header separator found")

    ciks, names, forms, dates, accessions, filenames = [], [], [], [], [], []
    for line in lines[start:]:
        parts = line.split("|")
        if len(parts) < 5:
            continue
        filename = parts[-1].strip()
        ciks.append(int(parts[0]))
        names.append("|".join(parts[1:-3]).strip())
        forms.append(parts[-3].strip())
        dates.append(parts[-2].strip())
        accessions.append(Path(filename).stem)
        filenames.append(filename)

    return pl.DataFrame(
        {
            "cik": ciks,
            "company_name": names,
            "form_type": forms,
            "date_filed": dates,
            "accession_number": accessions,
            "filename": filenames,
        },
        schema={**_FULL_INDEX_SCHEMA, "date_filed": pl.String},
    ).with_columns(pl.col("date_filed").str.to_date("%Y-%m-%d"))


class SecFullIndex(TableLike):
    """EDGAR quarterly full-index (`master.idx`): one row per filing, for
    every registrant that ever filed electronically since 1993 -- including
    those no longer active, which is what makes it survivorship-free.

    Stored hive-partitioned by year/quarter. Ingestion is always
    incremental: a quarter already downloaded after it ended (plus a few
    days to settle) is immutable and never downloaded again, so a rerun only
    fetches the current quarter and any quarter that was still open when it
    was last downloaded."""

    _URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz"

    def __init__(self, **kwargs) -> None:
        super().__init__(
            name="edgar_full_index",
            key_columns=["cik", "accession_number", "form_type"],
            partitioning_columns=["year", "quarter"],
            **kwargs,
        )

    def _partition_dir(self, year: int, quarter: int) -> Path:
        return self.root_dir / f"year={year}" / f"quarter={quarter}"

    def _is_final(self, year: int, quarter: int) -> bool:
        files = list(self._partition_dir(year, quarter).glob("*.parquet"))
        if not files:
            return False
        ingested_at = (
            pl.scan_parquet(files).select(pl.col("ingested_at").min()).collect().item()
        )
        settled = _quarter_end(year, quarter) + timedelta(days=_QUARTER_SETTLE_DAYS)
        return ingested_at is not None and ingested_at > settled

    def pending_quarters(self, today: date | None = None) -> list[tuple[int, int]]:
        """Quarters from 1993Q1 to today's quarter not yet stored as final."""
        today = today or date.today()
        last_year, last_quarter = _quarter_of(today)
        quarters = [
            (year, quarter)
            for year in range(FULL_INDEX_FIRST_YEAR, last_year + 1)
            for quarter in range(1, 5)
            if (year, quarter) <= (last_year, last_quarter)
        ]
        return [q for q in quarters if not self._is_final(*q)]

    def load_quarter(self, year: int, quarter: int, today: date) -> pl.DataFrame:
        url = self._URL.format(year=year, quarter=quarter)
        response = _sec_get(url)
        text = gzip.decompress(response.content).decode("latin-1")
        return parse_master_index(text).with_columns(
            pl.lit(year, pl.Int32).alias("year"),
            pl.lit(quarter, pl.Int32).alias("quarter"),
            pl.lit(today, pl.Date).alias("ingested_at"),
        )

    def load(self, **kwargs) -> Iterator[pl.DataFrame]:
        """Yields one DataFrame per pending quarter, persisted by `ingest()`
        one at a time (see `TableLike._persist`)."""
        today = kwargs.get("today") or date.today()
        pending = self.pending_quarters(today)
        logger.info("%s: %d quarter(s) to download", self.id, len(pending))
        for i, (year, quarter) in enumerate(pending, start=1):
            df = self.load_quarter(year, quarter, today)
            logger.info(
                "%s: %dQ%d downloaded, %d filings (%d/%d)",
                self.id,
                year,
                quarter,
                df.height,
                i,
                len(pending),
            )
            yield df

    def read_from_disk(self) -> pl.LazyFrame:
        return pl.scan_parquet(
            str(self.root_dir / "**" / "*.parquet"), hive_partitioning=True
        )


# --- monthly XBRL filing feeds (instance file names) --------------------------

XBRL_RSS_FIRST_MONTH = (2005, 4)
# the feeds' namespace changed from http:// to https:// in 2020
_EDGAR_NAMESPACES = (
    "{http://www.sec.gov/Archives/edgar}",
    "{https://www.sec.gov/Archives/edgar}",
)

_XBRL_FILINGS_SCHEMA = {
    "accession_number": pl.String,
    "cik": pl.Int64,
    "company_name": pl.String,
    "form_type": pl.String,
    "filing_date": pl.Date,
    "period": pl.String,
    "instance_file": pl.String,
    "schema_file": pl.String,
}


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def parse_xbrl_rss(content: bytes) -> pl.DataFrame:
    """Parses an EDGAR monthly XBRL feed (`xbrlrss-YYYY-MM.xml`): one row per
    XBRL filing with the names of its instance document (`EX-101.INS`, e.g.
    `aapl-20180929.xml`) and taxonomy schema (`EX-101.SCH`, e.g.
    `aapl-20230930.xsd`). Inline XBRL filings (from 2019) list no instance,
    but always a schema with the same prefix."""

    rows: dict[str, list] = {c: [] for c in _XBRL_FILINGS_SCHEMA}
    items = 0
    for _, element in ET.iterparse(io.BytesIO(content)):
        if element.tag == "item":
            items += 1
            continue
        ns = next(
            (n for n in _EDGAR_NAMESPACES if element.tag == n + "xbrlFiling"), None
        )
        if ns is None:
            continue

        def text(tag: str) -> str | None:
            value = element.findtext(ns + tag)
            return value.strip() if value and value.strip() else None

        files: dict[str, str] = {}
        for f in element.iter(ns + "xbrlFile"):
            # EX-101.* since 2009, EX-100.* in the 2005-2008 voluntary program
            kind = (f.get(ns + "type") or "").upper().split(".")[-1]
            files.setdefault(kind, f.get(ns + "file"))
        rows["accession_number"].append(text("accessionNumber"))
        rows["cik"].append(int(text("cikNumber") or 0) or None)
        rows["company_name"].append(text("companyName"))
        rows["form_type"].append(text("formType"))
        rows["filing_date"].append(text("filingDate"))
        rows["period"].append(text("period"))
        rows["instance_file"].append(files.get("INS"))
        rows["schema_file"].append(files.get("SCH"))
        element.clear()
    if items != len(rows["accession_number"]):
        # a format change would otherwise silently store empty months
        raise ValueError(
            f"XBRL feed has {items} items but {len(rows['accession_number'])} "
            "parsed filings"
        )

    return pl.DataFrame(
        rows, schema={**_XBRL_FILINGS_SCHEMA, "filing_date": pl.String}
    ).with_columns(pl.col("filing_date").str.to_date("%m/%d/%Y", strict=False))


class SecXbrlFilings(TableLike):
    """Every XBRL filing ever submitted to EDGAR, from the monthly feeds at
    `edgar/monthly/xbrlrss-YYYY-MM.xml` (from April 2005), with the file
    names of its XBRL instance document and taxonomy schema.

    Their prefix is the historical ticker source for filings whose cover
    page doesn't declare `dei:TradingSymbol` (mostly before 2019): the EDGAR
    Filer Manual asks for `<prefix>-<period date>.xml`, with the prefix "the
    registrant's ticker symbol or other mnemonic abbreviation". On filings
    that also declare their ticker, the prefix matches it ~97% of the time
    in every year since 2009. The DERA datasets have the instance name too
    (`sub.instance`), but re-downloading them would cost ~40GB against
    ~0.5GB of gzipped feeds.

    Stored hive-partitioned by year/month. Incremental like `SecFullIndex`:
    a month downloaded after it ended (plus a few days) is never fetched
    again."""

    _URL = "https://www.sec.gov/Archives/edgar/monthly/xbrlrss-{year}-{month:02d}.xml"

    def __init__(self, **kwargs) -> None:
        super().__init__(
            name="xbrl_filings",
            key_columns=["accession_number"],
            partitioning_columns=["year", "month"],
            **kwargs,
        )

    def _partition_dir(self, year: int, month: int) -> Path:
        return self.root_dir / f"year={year}" / f"month={month}"

    def _is_final(self, year: int, month: int) -> bool:
        files = list(self._partition_dir(year, month).glob("*.parquet"))
        if not files:
            return False
        ingested_at = (
            pl.scan_parquet(files).select(pl.col("ingested_at").min()).collect().item()
        )
        settled = _month_end(year, month) + timedelta(days=_QUARTER_SETTLE_DAYS)
        return ingested_at is not None and ingested_at > settled

    def pending_months(self, today: date | None = None) -> list[tuple[int, int]]:
        today = today or date.today()
        months = [
            (year, month)
            for year in range(XBRL_RSS_FIRST_MONTH[0], today.year + 1)
            for month in range(1, 13)
            if XBRL_RSS_FIRST_MONTH <= (year, month) <= (today.year, today.month)
        ]
        return [m for m in months if not self._is_final(*m)]

    def load_month(self, year: int, month: int, today: date) -> pl.DataFrame:
        response = _sec_get(self._URL.format(year=year, month=month))
        return (
            parse_xbrl_rss(response.content)
            .filter(pl.col("accession_number").is_not_null())
            .unique("accession_number", keep="first")
            .with_columns(
                pl.lit(year, pl.Int32).alias("year"),
                pl.lit(month, pl.Int32).alias("month"),
                pl.lit(today, pl.Date).alias("ingested_at"),
            )
        )

    def load(self, **kwargs) -> Iterator[pl.DataFrame]:
        today = kwargs.get("today") or date.today()
        pending = self.pending_months(today)
        logger.info("%s: %d month(s) to download", self.id, len(pending))
        for i, (year, month) in enumerate(pending, start=1):
            df = self.load_month(year, month, today)
            logger.info(
                "%s: %d-%02d downloaded, %d filings (%d/%d)",
                self.id,
                year,
                month,
                df.height,
                i,
                len(pending),
            )
            yield df

    def read_from_disk(self) -> pl.LazyFrame:
        return pl.scan_parquet(
            str(self.root_dir / "**" / "*.parquet"), hive_partitioning=True
        )


# --- XBRL cover pages (dei:TradingSymbol) -------------------------------------

# cover page facts kept from each filing: the ticker(s), the exchange each
# one trades on and the title of the security (to tell common stock from
# notes/preferreds), linked to each other by the filing's `dimh` (the hash of
# the security-class dimension the fact refers to)
COVER_PAGE_TAGS = ("TradingSymbol", "SecurityExchangeName", "Security12bTitle")

_NOTES_DATASETS_PAGE = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "financial-statement-notes-data-sets"
)
_NOTES_DATASET_HREF = re.compile(
    r'href="(/files/dera/data/financial-statement-notes-data-sets/'
    r'(\d{4}(?:q\d|_\d{2}))_notes\.zip)"'
)
_TXT_COLUMNS = (
    "adsh tag version ddate qtrs iprx lang dcml durp datp dimh dimn coreg "
    "escaped srclen txtlen footnote footlen context value"
).split()

_COVER_PAGES_SCHEMA = {
    "adsh": pl.String,
    "cik": pl.Int64,
    "company_name": pl.String,
    "form": pl.String,
    "period": pl.String,
    "filed": pl.Date,
    "tag": pl.String,
    "ddate": pl.String,
    "dimh": pl.String,
    "coreg": pl.String,
    "context": pl.String,
    "value": pl.String,
}


def _zip_member(z: zipfile.ZipFile, stem: str) -> str:
    """The dataset's `<stem>.tsv` (or `.txt`, in older datasets)."""
    for name in z.namelist():
        if Path(name).stem == stem:
            return name
    raise KeyError(f"no {stem}.tsv in dataset")


def parse_notes_dataset(zip_file) -> pl.DataFrame:
    """Extracts the cover page facts (`COVER_PAGE_TAGS`) from a DERA
    "Financial Statement and Notes" dataset zip (path or file object),
    joined with the filing they come from (`sub.tsv`).

    `txt.tsv` holds every non-numeric fact of every filing (up to a couple
    of GB per dataset), so it's streamed line by line and only the few cover
    page rows are kept."""
    with zipfile.ZipFile(zip_file) as z:
        sub = pl.read_csv(
            z.read(_zip_member(z, "sub")),
            separator="\t",
            quote_char=None,
            infer_schema=False,
            columns=["adsh", "cik", "name", "form", "period", "filed"],
            encoding="utf8-lossy",
        )

        rows: dict[str, list[str]] = {
            c: [] for c in ("adsh", "tag", "ddate", "dimh", "coreg", "context", "value")
        }
        needles = tuple(f"\t{tag}\t" for tag in COVER_PAGE_TAGS)
        tag_index, n_columns = _TXT_COLUMNS.index("tag"), len(_TXT_COLUMNS)
        with z.open(_zip_member(z, "txt")) as raw:
            lines = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            next(lines)  # header
            for line in lines:
                if not any(needle in line for needle in needles):
                    continue
                fields = line.rstrip("\r\n").split("\t")
                if len(fields) != n_columns or fields[tag_index] not in COVER_PAGE_TAGS:
                    continue
                record = dict(zip(_TXT_COLUMNS, fields))
                for column, values in rows.items():
                    values.append(record[column])

    facts = pl.DataFrame(rows, schema={c: pl.String for c in rows})
    return (
        facts.join(sub, on="adsh", how="inner")
        .with_columns(
            pl.col("cik").cast(pl.Int64),
            pl.col("filed").str.to_date("%Y%m%d"),
            pl.col("value").str.strip_chars(),
        )
        .rename({"name": "company_name"})
        .select(list(_COVER_PAGES_SCHEMA))
    )


class SecXbrlCoverPages(TableLike):
    """Ticker(s) and exchange(s) declared on the XBRL cover page of every
    filing (`dei:TradingSymbol`, `dei:SecurityExchangeName`,
    `dei:Security12bTitle`), extracted from DERA's "Financial Statement and
    Notes" datasets (quarterly from 2009, monthly from late 2020).

    This is the only official bulk source of *historical* tickers: XBRL
    `companyfacts` only has numeric facts, and `submissions.zip` only has
    current tickers. Coverage is partial before 2019, when tagging the
    cover page became mandatory (10-K, 10-Q, 8-K, 20-F, ...).

    Stored hive-partitioned by dataset. Published datasets never change, so
    ingestion is always incremental: only datasets not on disk yet are
    downloaded (each zip is streamed to a temporary file and deleted once
    parsed)."""

    def __init__(self, **kwargs) -> None:
        super().__init__(
            name="xbrl_cover_pages",
            key_columns=["adsh", "tag", "dimh", "coreg", "ddate", "context"],
            partitioning_columns=["dataset"],
            **kwargs,
        )

    def available_datasets(self) -> dict[str, str]:
        """Dataset name (e.g. `2019q1`, `2026_10`) -> download URL."""
        html = _sec_get(_NOTES_DATASETS_PAGE).text
        return {
            name: f"https://www.sec.gov{href}"
            for href, name in _NOTES_DATASET_HREF.findall(html)
        }

    def stored_datasets(self) -> set[str]:
        return {
            p.name.removeprefix("dataset=")
            for p in self.root_dir.glob("dataset=*")
            if any(p.glob("*.parquet"))
        }

    def load_dataset(self, url: str) -> pl.DataFrame:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=self.root_dir) as tmp:
            with _sec_get(url, stream=True) as response:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    tmp.write(chunk)
            tmp.seek(0)
            return parse_notes_dataset(tmp)

    def load(self, **kwargs) -> Iterator[pl.DataFrame]:
        """Yields one DataFrame per dataset not stored yet."""
        available = self.available_datasets()
        if not available:
            raise RuntimeError(f"no dataset found at {_NOTES_DATASETS_PAGE}")
        stored = self.stored_datasets()
        pending = sorted(set(available) - stored)
        logger.info("%s: %d dataset(s) to download", self.id, len(pending))
        for i, name in enumerate(pending, start=1):
            df = self.load_dataset(available[name])
            logger.info(
                "%s: dataset %s parsed, %d cover page facts (%d/%d)",
                self.id,
                name,
                df.height,
                i,
                len(pending),
            )
            yield df.with_columns(pl.lit(name).alias("dataset"))

    def read_from_disk(self) -> pl.LazyFrame:
        return pl.scan_parquet(
            str(self.root_dir / "**" / "*.parquet"), hive_partitioning=True
        )
