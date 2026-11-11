"""`silver.security_master`: every company that ever filed periodic reports
with the SEC, keyed by its permanent CIK, with the ticker(s) it traded under
over time -- the survivorship-free universe of US registrants.

Each row says "between `valid_from` (inclusive) and `valid_to` (exclusive,
null = still valid) company `cik` was called `company_name` and traded as
`ticker`". A company with several share classes (GOOG/GOOGL) has one row per
class over the same period; a ticker change (FB -> META) closes one row and
opens the next; periods of a company's life with no known ticker have a row
with a null `ticker`.

How it's built (see `docs/data/security_master.md` for the rationale):

- **Existence** comes from the EDGAR full-index: a company exists from its
  first periodic report (10-K/10-Q/20-F/40-F and their small-business and
  transition variants) or exchange registration (8-A, Form 10, 20-FR),
  whichever comes first, until it is delisted (Form 25/25-NSE),
  deregistered (Form 15) or stops reporting. Companies in today's
  `company_tickers.json` are always still alive.
- **Tickers** come from the `dei:TradingSymbol` each filing declares on its
  XBRL cover page (2009 onwards, mandatory since 2019), from the prefix of
  its XBRL file names (`cpwr-20131231.xml`, see `_filename_events` for when
  a prefix is trusted) and from today's `company_tickers.json`. A ticker is
  valid from the first filing showing it until the first later filing of
  the same company that no longer does. Before the first observation the
  earliest ticker is assumed to go back to the company's start, but not
  past a change of company name or a long silence (`source = "backfill"`,
  see `_backfill_start`).
- **Conflicts**: a ticker belongs to one company at a time. When two
  companies claim it over the same period, today's owner wins, then the
  stronger evidence (declared, file name, backfill), then an exchange-listed
  company, then the one seen with it more often (see
  `_resolve_ticker_conflicts`).
"""

import re
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from etl.ingestion.sec import (
    SecFilingTextTickers,
    clean_symbol,
    SecFullIndex,
    SecTickers,
    SecTickersExchange,
    SecXbrlCoverPages,
    SecXbrlFilings,
)
from etl.logger import get_logger
from etl.transformation.model import DEFAULT_DATAPLATFORM_ROOT, Model
from etl.transformation.quality_checks import (
    accepted_values,
    comparison,
    matches_regex,
    min_share,
    no_overlapping_intervals,
    not_empty,
    not_null,
    unique,
    with_root,
)
from etl.transformation.silver.company_tickers import CompanyTickersSilver

logger = get_logger(__name__)

# periodic reports: filing one means the company had reporting obligations
PERIODIC_FORMS = [
    "10-K",
    "10-K405",
    "10-KT",
    "10KT405",
    "10-KSB",
    "10KSB",
    "10-KSB40",
    "10KSB40",
    "10-Q",
    "10-QT",
    "10-QSB",
    "10QSB",
    "20-F",
    "40-F",
]
# foreign private issuers only report once a year
ANNUAL_ONLY_FORMS = ["20-F", "40-F"]
# registration of a class of securities (on an exchange or under 12(g)):
# usually filed right before listing, i.e. before the first periodic report
REGISTRATION_FORMS = [
    "8-A12B",
    "8-A12G",
    "10-12B",
    "10-12G",
    "10SB12B",
    "10SB12G",
    "20FR12B",
    "20FR12G",
    # successor issuers (e.g. a new holding company: Google -> Alphabet)
    "8-K12B",
    "8-K12G3",
    "8-K15D5",
]
DELISTING_FORMS = ["25", "25-NSE"]
DEREGISTRATION_FORMS = ["15-12B", "15-12G", "15-15D", "15F-12B", "15F-12G", "15F-15D"]

# only a Form 25/15 filed around the end of a company's reporting ends its
# life (see `_terminal_delisting`/`_terminal_deregistration`): up to this
# long before its last periodic report (delisted, then one or two final
# reports) or after it. Earlier ones, followed by years of reports, delisted
# a single class (e.g. notes) or came with a transfer to another exchange.
TERMINAL_EVENT_WINDOW = timedelta(days=365)
# a company that stops filing without a Form 25/15 is considered gone this
# long after its last report: the longest normal gap between two reports
# (Q3 10-Q to 10-K) is ~4.5 months, a year for 20-F/40-F filers
QUARTERLY_REPORTER_GRACE = timedelta(days=183)
ANNUAL_REPORTER_GRACE = timedelta(days=456)

SOURCES = [
    "full-index",
    "xbrl",
    "xbrl_filename",
    "filing_text",
    "company_tickers",
    "backfill",
]
# evidence strength in ticker conflicts (lower wins, see `_Interval.priority`)
_SOURCE_RANK = {
    "xbrl": 0,
    "company_tickers": 0,
    "xbrl_filename": 1,
    "filing_text": 2,
    "backfill": 3,
}

# --- tickers from XBRL file names ---
# only periodic reports: 8-K/6-K XBRL is often produced from filing agents'
# templates whose file names carry the agent's mnemonic (e.g.
# `gnw-20190101.xsd`, GlobeNewswire), not the company's
FILENAME_TICKER_FORMS = PERIODIC_FORMS + [f + "/A" for f in PERIODIC_FORMS]
# `<prefix>-<yyyymmdd>`: a prefix shaped like a ticker followed by the period
_FILENAME_PREFIX = r"^([a-z]{1,5})[-_]\d{8}"
_PLACEHOLDER_PREFIXES = [
    "NONE",
    "NA",
    "XBRL",
    "TEST",
    "FIL",
    "FORM",
    "DOC",
    "CIK",
    "CK",
    "TBD",
]
# a real ticker belongs to one company at a time: a prefix used by this many
# companies in the same year is a filing agent's template; used by two, it
# is a parent's ticker reused by a subsidiary (or a mnemonic colliding with
# a ticker), and only goes to the one with publicly held shares
SHARED_PREFIX_MIN_COMPANIES = 3
# a file-name ticker that another company already declares within this long
# of its first use was never this company's ticker
MNEMONIC_OVERLAP_WINDOW = timedelta(days=365)
# two companies whose use of a prefix overlaps at least this long used it
# at the same time (less is one taking over from the other)
SIMULTANEOUS_USE = timedelta(days=120)
# a prefix seen on a single filing is too weak to be backfilled for decades
MIN_FILENAME_OBSERVATIONS = 2
# evidence of publicly held shares: 5% ownership reports (only filed about a
# registered class of equity) and proxy statements
EQUITY_FORMS = [
    "SC 13G",
    "SC 13G/A",
    "SC 13D",
    "SC 13D/A",
    "SCHEDULE 13G",
    "SCHEDULE 13G/A",
    "SCHEDULE 13D",
    "SCHEDULE 13D/A",
    "DEF 14A",
    "DEFA14A",
]
# a company whose equity evidence stopped this many years before a filing
# went private or became a subsidiary, and keeps filing only for its debt:
# its file-name prefix is its former ticker (e.g. Puget Energy, private
# since 2009, still files as `psd`)
# (on listed company-years, i.e. declaring an exchange, the rule misfires
# 1.3% of the time: 1.8% with 1 year, 1.1% with 3)
EQUITY_LAPSE_YEARS = 2

# reports filed this long after a company's delisting/deregistration (its
# final 10-Q/10-K) still show the ticker it had when it left
FINAL_REPORT_WINDOW = timedelta(days=365)

# --- backfill ---
# a company that stopped reporting this long and came back is usually a
# shell revived under a new business (and a new ticker)
REPORTING_GAP_RESET = timedelta(days=730)
# forms showing a company was exchange-listed at some point: it wins ticker
# conflicts against companies that never were (non-traded REITs, private
# issuers of public debt, whose file-name prefix is just a mnemonic)
LISTING_FORMS = ["8-A12B", "20FR12B", "8-K12B", "25", "25-NSE"]
TICKER_PATTERN = r"[A-Z0-9]{1,6}([.\-][A-Z0-9]{1,4})?"
_TICKER_RE = re.compile(f"^{TICKER_PATTERN}$")
_NOT_A_TICKER = {"NONE", "N/A", "N.A.", "NOTAPPLICABLE", "-", "NIL", "TBD", "FALSE"}
# placeholders that are also real tickers (Nano Labs is NA, TrueCar TRUE):
# a declared one only counts when the company holds it today
_AMBIGUOUS_TICKERS = {"NA", "TRUE"}
# cover page tagging became mandatory (with the exchange of each class) in
# 2019; before, `TradingSymbol` was optional and sometimes filled with a
# private company's old ticker or mnemonic
MANDATORY_COVER_PAGE_DATE = date(2019, 6, 15)

# dei:SecurityExchangeName enumeration -> name used in company_tickers
_EXCHANGE_NAMES = {
    "NASDAQ": "NASDAQ",
    "NYSE": "NYSE",
    "NYSEAMER": "NYSE AMERICAN",
    "NYSEARCA": "NYSE ARCA",
    "NYSENAT": "NYSE NATIONAL",
    "CBOE": "CBOE",
    "BOX": "BOX",
    "CHX": "CHX",
    "IEX": "IEX",
    "PHLX": "PHLX",
    "BX": "NASDAQ BX",
    "MIAX": "MIAX",
    "TXSE": "TXSE",
}

_SCHEMA = {
    "cik": pl.Int64,
    "company_name": pl.String,
    "ticker": pl.String,
    "exchange": pl.String,
    "valid_from": pl.Date,
    "valid_to": pl.Date,
    "first_filing_date": pl.Date,
    "last_filing_date": pl.Date,
    "delisting_date": pl.Date,
    "deregistration_date": pl.Date,
    "delisting_reason": pl.String,
    "has_known_ticker": pl.Boolean,
    "source": pl.String,
}


# --- inputs -------------------------------------------------------------------


def _company_lifecycles(filings: pl.LazyFrame) -> pl.DataFrame:
    """One row per CIK with the dates of the filings that bound its life."""
    form, filed = pl.col("form_type"), pl.col("date_filed")
    periodic = form.is_in(PERIODIC_FORMS)
    return (
        filings.group_by("cik")
        .agg(
            filed.filter(periodic).min().alias("first_filing_date"),
            filed.filter(periodic).max().alias("last_filing_date"),
            form.filter(periodic)
            .sort_by(filed.filter(periodic))
            .last()
            .alias("last_periodic_form"),
            filed.filter(form.is_in(REGISTRATION_FORMS))
            .min()
            .alias("first_registration_date"),
            filed.filter(form.is_in(DELISTING_FORMS))
            .unique()
            .sort()
            .alias("delistings"),
            filed.filter(form.is_in(DEREGISTRATION_FORMS))
            .unique()
            .sort()
            .alias("deregistrations"),
            filed.min().alias("first_any_filing"),
            form.is_in(LISTING_FORMS).any().alias("listed"),
        )
        .collect(engine="streaming")
    )


def _equity_years(filings: pl.LazyFrame) -> dict[int, dict[int, int]]:
    """cik -> {year: number of filings} evidencing publicly held shares
    (`EQUITY_FORMS` filed by or about the company), years in order."""
    years = (
        filings.filter(pl.col("form_type").is_in(EQUITY_FORMS))
        .group_by("cik", pl.col("date_filed").dt.year().alias("year"))
        .agg(pl.len().alias("n"))
        .sort("cik", "year")
        .collect(engine="streaming")
    )
    by_cik: dict[int, dict[int, int]] = defaultdict(dict)
    for cik, year, n in years.iter_rows():
        by_cik[cik][year] = n
    return by_cik


def _equity_lapsed(years, day: date) -> bool:
    """Whether a company with equity evidence in `years` (sorted) had
    stopped having publicly held shares by `day`: it had some, and none in
    the `EQUITY_LAPSE_YEARS` before."""
    before = [y for y in years if y <= day.year + 1]
    return bool(before) and max(before) < day.year - EQUITY_LAPSE_YEARS


def _reporting_resumptions(filings: pl.LazyFrame) -> pl.DataFrame:
    """(cik, resumed): periodic reports filed after a silence longer than
    `REPORTING_GAP_RESET`."""
    periodic = (
        filings.filter(pl.col("form_type").is_in(PERIODIC_FORMS))
        .select("cik", "date_filed")
        .unique()
        .sort("cik", "date_filed")
    )
    return (
        periodic.with_columns(
            pl.col("date_filed").shift(1).over("cik").alias("previous")
        )
        .filter(pl.col("date_filed") - pl.col("previous") > REPORTING_GAP_RESET)
        .select("cik", pl.col("date_filed").alias("resumed"))
        .collect(engine="streaming")
    )


# words that don't make two names different companies: legal forms, state of
# incorporation tags ("/DE/") and holding-company wrappers
_NAME_NOISE = re.compile(
    r"\b(?:INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|COS|COMPANIES|LTD|"
    r"LIMITED|PLC|LP|LLC|L ?P|L ?L ?C|SA|NV|AG|SE|AB|ASA|ADR|THE|NEW|HOLDING|"
    r"HOLDINGS|GROUP|TRUST|BANCORP|BANCORPORATION|DE|MD|NV|NJ|NY|CA|PA|TX|FL|"
    r"MN|MA|OH|VA|WA|CO|DEL|DELAWARE)\b"
)


def company_name_key(name: str | None) -> str:
    """Key telling whether two names are the same company: case, punctuation,
    `/STATE/` tags, legal forms and holding wrappers are ignored, so
    `COMPUWARE CORPORATION` and `Compuware Corp /MI/` match while `TETRIDYN
    SOLUTIONS INC` and `Ocean Thermal Energy Corp` don't."""
    if not name:
        return ""
    key = name.upper().replace("&", " AND ")
    key = re.sub(r"/[A-Z ]{1,6}/?", " ", key)
    key = re.sub(r"[^A-Z0-9 ]", " ", key)
    key = _NAME_NOISE.sub(" ", key)
    return re.sub(r"\s+", " ", key).strip()


def _name_history(filings: pl.LazyFrame, ciks: pl.DataFrame) -> pl.DataFrame:
    """(cik, since, company_name): the name EDGAR shows on each company's
    filings, one row per distinct name (ignoring case/punctuation), valid
    from the first filing carrying it. A name seen again after a different
    one (index noise) doesn't reopen it."""
    normalized = (
        pl.col("company_name")
        .str.to_uppercase()
        .str.replace_all(r"[^A-Z0-9 ]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )
    return (
        filings.join(ciks.lazy(), on="cik", how="semi")
        .with_columns(normalized.alias("_name_key"))
        .group_by("cik", "_name_key")
        .agg(
            pl.col("date_filed").min().alias("since"),
            pl.col("company_name").sort_by("date_filed").last(),
        )
        .select("cik", "since", "company_name")
        .collect(engine="streaming")
    )


def _canonical_exchange(value: pl.Expr) -> pl.Expr:
    upper = value.str.to_uppercase().str.strip_chars()
    return upper.replace(_EXCHANGE_NAMES)


def _ticker_declarations(cover_pages: pl.LazyFrame) -> pl.DataFrame:
    """(cik, adsh, filed, raw_ticker, exchange): one row per ticker declared
    on a filing's XBRL cover page by the filer itself (facts of
    co-registrants are left out), paired with the exchange declared for the
    same security class."""
    own = cover_pages.filter(pl.col("coreg").fill_null("") == "")
    symbols = own.filter(pl.col("tag") == "TradingSymbol").select(
        "cik", "adsh", "filed", "dimh", pl.col("value").alias("raw_ticker")
    )
    exchanges = (
        own.filter(pl.col("tag") == "SecurityExchangeName")
        .select("adsh", "dimh", _canonical_exchange(pl.col("value")).alias("exchange"))
        .unique(["adsh", "dimh"], keep="first")
    )
    return (
        symbols.join(exchanges, on=["adsh", "dimh"], how="left")
        .select("cik", "adsh", "filed", "raw_ticker", "exchange")
        .unique()
        .collect()
    )


def _coregistrants(filings: pl.LazyFrame) -> pl.DataFrame:
    """(accession_number, cik) of the periodic reports filed jointly by
    several companies (a holding company and its subsidiaries filing one
    combined 10-K), from the full-index."""
    if "accession_number" not in filings.collect_schema().names():
        return pl.DataFrame(schema={"accession_number": pl.String, "cik": pl.Int64})
    return (
        filings.filter(pl.col("form_type").is_in(FILENAME_TICKER_FORMS))
        .select("accession_number", "cik")
        .drop_nulls()
        .unique()
        .filter(pl.len().over("accession_number") > 1)
        .collect(engine="streaming")
    )


def _filename_prefixes(
    xbrl_filings: pl.LazyFrame, coregistrants: pl.DataFrame | None = None
) -> pl.DataFrame:
    """(cik, accession_number, filed, prefix): the ticker-shaped prefix of
    the XBRL file names of each periodic report (schema file, which every
    filing has, else instance), uppercased. Placeholders and prefixes shared
    by several companies in the same year are dropped.

    The XBRL feeds list a combined filing under one of its co-registrants
    only (often a subsidiary): its prefix is attributed to all of them
    (`coregistrants`), and `_filename_events` gives it to the one with
    publicly held shares."""
    file = pl.coalesce("schema_file", "instance_file").str.to_lowercase()
    prefixes = (
        xbrl_filings.filter(pl.col("form_type").is_in(FILENAME_TICKER_FORMS))
        .select(
            "cik",
            "accession_number",
            pl.col("filing_date").alias("filed"),
            file.str.extract(_FILENAME_PREFIX, 1).str.to_uppercase().alias("prefix"),
        )
        .filter(
            pl.col("prefix").is_not_null(),
            pl.col("filed").is_not_null(),
            ~pl.col("prefix").is_in(_PLACEHOLDER_PREFIXES),
        )
        .collect()
    )
    shared = (
        prefixes.group_by(pl.col("filed").dt.year().alias("year"), "prefix")
        .agg(pl.col("cik").n_unique().alias("companies"))
        .filter(pl.col("companies") >= SHARED_PREFIX_MIN_COMPANIES)
    )
    prefixes = (
        prefixes.with_columns(pl.col("filed").dt.year().alias("year"))
        .join(shared, on=["year", "prefix"], how="anti")
        .drop("year")
    )
    if coregistrants is None or coregistrants.is_empty():
        return prefixes
    return (
        prefixes.join(coregistrants, on="accession_number", how="left", suffix="_co")
        .with_columns(pl.coalesce("cik_co", "cik").alias("cik"))
        .drop("cik_co")
        .unique()
    )


_EXCHANGE_WORDS = r"(?:NYSE|NASDAQ|AMEX|CBOE|OTC|ARCA|NYSE ?AMERICAN|NYSE ?ARCA|NASDAQ ?[GC]?[SM]|[A-Z ]*EXCHANGE)"
_EXCHANGE_PREFIX = re.compile(rf"^{_EXCHANGE_WORDS}\s*:\s*")
_EXCHANGE_SUFFIX = re.compile(
    rf"\s*(?:\(\s*{_EXCHANGE_WORDS}\s*\)|-\s+{_EXCHANGE_WORDS})$"
)


def _ticker_key(ticker: str) -> str:
    """Spelling-insensitive key: BRK.B, BRK-B, BRK B and BRKB all match."""
    return re.sub(r"[^A-Z0-9]", "", ticker.upper())


def normalize_tickers(
    raw: str | None, known: dict[str, str] | None = None
) -> list[str]:
    """Splits a declared `dei:TradingSymbol` (which sometimes lists several
    symbols, or uses dots/spaces for share classes) into valid tickers.

    A symbol matching one of the company's current tickers up to
    punctuation (`known`: key -> current spelling) takes that spelling;
    otherwise class separators become `-`, like in `company_tickers.json`.
    Placeholders (`N/A`, `NONE`, ...) and anything not shaped like a ticker
    are dropped."""
    if raw is None:
        return []
    known = known or {}
    tickers = []
    for piece in re.split(r"[,;&]|\bAND\b", raw.upper()):
        piece = _EXCHANGE_PREFIX.sub("", piece.strip().strip("\"'*. "))
        # "BAX (NYSE)", "XYZ - Nasdaq": drop the exchange; "(VIXM)": unwrap
        piece = _EXCHANGE_SUFFIX.sub("", piece)
        piece = piece.removeprefix("(").removesuffix(")").strip()
        if not piece or piece in _NOT_A_TICKER:
            continue
        key = _ticker_key(piece)
        if piece in _AMBIGUOUS_TICKERS and key not in known:
            continue
        if key in known:
            ticker = known[key]
        else:
            ticker = re.sub(r"[\s./]+", "-", piece)
        if (
            _TICKER_RE.match(ticker)
            and re.search(r"[A-Z]", ticker)
            and ticker not in tickers
        ):
            tickers.append(ticker)
    return tickers


# --- intervals ----------------------------------------------------------------


@dataclass
class _Interval:
    cik: int
    ticker: str
    start: date
    end: date | None  # exclusive, None = open-ended
    source: str
    exchange: str | None
    current_owner: bool = False
    observations: int = 0
    listed: bool = False

    def priority(self) -> tuple:
        """Sort key: lower wins a conflict over the same ticker. Today's
        owner first, then the stronger evidence (declared, then file name,
        then backfill), then a company that was exchange-listed, then the
        one seen with the ticker more often."""
        return (
            not self.current_owner,
            _SOURCE_RANK[self.source],
            not self.listed,
            -self.observations,
            -self.start.toordinal(),
        )


def _clip(start: date, end: date | None, lo: date, hi: date | None):
    """[start, end) intersected with [lo, hi), or None if empty."""
    new_start = max(start, lo)
    ends = [e for e in (end, hi) if e is not None]
    new_end = min(ends) if ends else None
    if new_end is not None and new_end <= new_start:
        return None
    return new_start, new_end


def _subtract(
    start: date, end: date | None, taken: list[tuple[date, date | None]]
) -> list[tuple[date, date | None]]:
    """Parts of [start, end) not covered by any interval in `taken`."""
    pieces = [(start, end)]
    for t_start, t_end in taken:
        remaining = []
        for p_start, p_end in pieces:
            overlaps = (t_end is None or p_start < t_end) and (
                p_end is None or t_start < p_end
            )
            if not overlaps:
                remaining.append((p_start, p_end))
                continue
            if p_start < t_start:
                remaining.append((p_start, t_start))
            if t_end is not None and (p_end is None or t_end < p_end):
                remaining.append((t_end, p_end))
        pieces = remaining
    return pieces


def _widened(interval: _Interval, margin: timedelta) -> _Interval:
    return _Interval(
        interval.cik,
        interval.ticker,
        interval.start - margin,
        None if interval.end is None else interval.end + margin,
        interval.source,
        interval.exchange,
    )


def _resolve_ticker_conflicts(intervals: list[_Interval]) -> list[_Interval]:
    """Makes each ticker belong to at most one company at a time: intervals
    are granted in priority order (see `_Interval.priority`), each keeping
    only the parts not already granted to another company.

    Some claims are dropped whole instead of trimmed: a file-name claim
    whose very start overlaps another company's declared or current use of
    the ticker (the prefix was a mnemonic; a later overlap is just a former
    owner's stale prefix and is trimmed), a text claim within a year of
    another company's observed use (the report named another company's
    symbol), and a backfill whose company kept nothing of the ticker it
    extends."""
    by_ticker: dict[str, list[_Interval]] = defaultdict(list)
    for interval in intervals:
        by_ticker[interval.ticker].append(interval)

    def overlaps(a: _Interval, b: _Interval) -> bool:
        return (a.end is None or b.start < a.end) and (b.end is None or a.start < b.end)

    resolved = []
    for claims in by_ticker.values():
        claims.sort(key=_Interval.priority)
        granted: list[_Interval] = []
        for claim in claims:
            if claim.source == "xbrl_filename" and any(
                g.cik != claim.cik
                and _SOURCE_RANK[g.source] < _SOURCE_RANK[claim.source]
                and g.source != "backfill"
                and overlaps(g, claim)
                and g.start <= claim.start + MNEMONIC_OVERLAP_WINDOW
                for g in granted
            ):
                continue
            # an annual report also names its subsidiaries', parent's or
            # acquirer's symbols: a text ticker that another company shows
            # with stronger evidence within a year was that company's
            if claim.source == "filing_text" and any(
                g.cik != claim.cik
                and g.source != "backfill"
                and overlaps(g, _widened(claim, MNEMONIC_OVERLAP_WINDOW))
                for g in granted
            ):
                continue
            if claim.source == "backfill" and not any(
                g.cik == claim.cik and g.source != "backfill" for g in granted
            ):
                continue
            others = [(g.start, g.end) for g in granted if g.cik != claim.cik]
            for start, end in _subtract(claim.start, claim.end, others):
                granted.append(
                    _Interval(
                        claim.cik,
                        claim.ticker,
                        start,
                        end,
                        claim.source,
                        claim.exchange,
                        claim.current_owner,
                        claim.observations,
                        claim.listed,
                    )
                )
        resolved.extend(granted)
    return resolved


def _company_span(
    life: dict, first_declaration: date | None, current: bool, snapshot_date: date
) -> tuple[date, date | None]:
    """[start, end) of a company's life (see module docstring). `life` holds
    the company's `_company_lifecycles` row plus its terminal
    `delisting_date`/`deregistration_date` and `last_declaration`."""
    starts = [
        d
        for d in (
            life.get("first_filing_date"),
            life.get("first_registration_date"),
            first_declaration,
        )
        if d is not None
    ]
    if not starts and current:
        starts = [life.get("first_any_filing") or snapshot_date]
    start = min(starts)
    if current:
        return start, None

    last_report = life.get("last_filing_date")
    terminal_events = [
        event
        for event in (life.get("delisting_date"), life.get("deregistration_date"))
        if event is not None
    ]
    if terminal_events:
        end = min(terminal_events)
        # a Form 25/15 filed before the first periodic report: the company's
        # life predates EDGAR and the report was a final one (zero-length
        # span, the company is never alive in the data)
        return (start, end) if end > start else (end, end)

    last_activity = max(
        d for d in (last_report, life.get("last_declaration"), start) if d is not None
    )
    end = last_activity + _reporting_grace(life.get("last_periodic_form"))
    return start, (None if end > snapshot_date else end)


def _reporting_grace(last_periodic_form: str | None) -> timedelta:
    if last_periodic_form in ANNUAL_ONLY_FORMS:
        return ANNUAL_REPORTER_GRACE
    return QUARTERLY_REPORTER_GRACE


def _terminal_deregistration(
    events: list[date] | None, last_report: date | None
) -> date | None:
    """Among a company's Form 15 dates, the one that ended its reporting:
    the first filed after its last periodic report, or failing that the
    last one filed shortly before it (see `TERMINAL_EVENT_WINDOW`)."""
    if not events:
        return None
    if last_report is None:
        return min(events)
    after = [e for e in events if e >= last_report]
    if after:
        return min(after)
    shortly_before = [e for e in events if e >= last_report - TERMINAL_EVENT_WINDOW]
    return max(shortly_before) if shortly_before else None


def _terminal_delisting(
    events: list[date] | None, last_report: date | None, grace: timedelta
) -> date | None:
    """Among a company's Form 25 dates, the one that ended its listing: the
    last one filed between `TERMINAL_EVENT_WINDOW` before its last periodic
    report and the date its next report was due (`last_report + grace`).

    Issuers with listed notes get a Form 25 every time a series matures
    (Lehman Brothers had dozens), so the first Form 25 after the last report
    is often a note's, months before the stock's; taking the last one errs
    on keeping a dying company in the universe a bit longer, never on
    dropping it before its collapse. Failing that, the first one filed
    afterwards (still listed long after its last report)."""
    if not events:
        return None
    if last_report is None:
        return min(events)
    window_start, window_end = last_report - TERMINAL_EVENT_WINDOW, last_report + grace
    in_window = [e for e in events if window_start <= e <= window_end]
    if in_window:
        return max(in_window)
    after = [e for e in events if e > window_end]
    return min(after) if after else None


def _backfill_start(
    first_observation: date,
    span_start: date,
    names: list[tuple[date, str]],
    resumptions: list[date],
) -> date:
    """How far back a company's earliest observed ticker is assumed to go:
    to the company's start, but not past a change of company name (a new
    name usually comes with a new ticker, and a CIK can be a shell reused
    by an unrelated business) nor past a long silence in its reporting
    (`REPORTING_GAP_RESET`)."""
    start = span_start
    current = [i for i, (since, _) in enumerate(names) if since <= first_observation]
    if current:
        k = current[-1]
        key = company_name_key(names[k][1])
        j = k
        while j > 0 and company_name_key(names[j - 1][1]) == key:
            j -= 1
        if j > 0:
            start = max(start, names[j][0])
    resumed = [d for d in resumptions if d <= first_observation]
    if resumed:
        start = max(start, resumed[-1])
    return start


def _ticker_intervals(
    cik: int,
    events: list[tuple[date, frozenset[str], bool]],
    observations: dict[str, int],
    exchanges: dict[str, str | None],
    snapshot: dict[str, str | None] | None,
    snapshot_date: date,
    span: tuple[date, date | None],
    names: list[tuple[date, str]] | None = None,
    resumptions: list[date] | None = None,
    listed: bool = False,
) -> list[_Interval]:
    """Validity intervals of each ticker a company showed (`events`: one
    (filing date, tickers, kind) per filing date, sorted; `kind` is the
    source the tickers come from: `xbrl` (cover page), `xbrl_filename` or
    `filing_text`) or currently has (`snapshot`: ticker -> exchange, None
    if the company isn't current).

    A ticker is valid from the first filing showing it until the first
    later filing that doesn't. The part before its first declaration on a
    cover page keeps the source of its first observation."""
    seen: dict[str, list[date]] = defaultdict(list)
    first_declared: dict[str, date] = {}
    first_kind: dict[str, str] = {}
    for day, tickers, kind in events:
        for ticker in tickers:
            seen[ticker].append(day)
            first_kind.setdefault(ticker, kind)
            if kind == "xbrl":
                first_declared.setdefault(ticker, day)

    intervals = []

    def add(ticker, start, end, source, current_owner, n):
        intervals.append(
            _Interval(
                cik,
                ticker,
                start,
                end,
                source,
                exchanges.get(ticker),
                current_owner=current_owner,
                observations=n,
                listed=listed,
            )
        )

    for ticker, days in seen.items():
        first, last = days[0], days[-1]
        current_owner = snapshot is not None and ticker in snapshot
        if current_owner:
            # still the company's ticker today, whatever filings in between
            # omitted (or mangled) it
            end = None
        else:
            end = next(
                (d for d, ts, _ in events if d > last and ticker not in ts), None
            )
            if end is None and snapshot is not None and snapshot_date > last:
                # the company is current but today's mapping no longer lists it
                end = snapshot_date
        declared_from = first_declared.get(ticker)
        if declared_from is None:
            add(ticker, first, end, first_kind[ticker], current_owner, len(days))
        elif declared_from == first:
            add(ticker, first, end, "xbrl", current_owner, len(days))
        else:
            add(
                ticker,
                first,
                declared_from,
                first_kind[ticker],
                current_owner,
                len(days),
            )
            add(ticker, declared_from, end, "xbrl", current_owner, len(days))

    for ticker, exchange in (snapshot or {}).items():
        if ticker in seen:
            continue
        intervals.append(
            _Interval(
                cik,
                ticker,
                snapshot_date,
                None,
                "company_tickers",
                exchange,
                current_owner=True,
                listed=listed,
            )
        )

    # a company whose only reports showing a ticker were filed after it was
    # delisted (its final 10-Q/10-K) still had that ticker when it left
    end = span[1]
    if end is not None and intervals and all(i.start >= end for i in intervals):
        final: dict[str, _Interval] = {}
        for i in sorted(intervals, key=lambda i: _SOURCE_RANK[i.source]):
            if i.start <= end + FINAL_REPORT_WINDOW and i.ticker not in final:
                i.start, i.end = end - timedelta(days=1), end
                final[i.ticker] = i
        intervals = list(final.values())

    # before its first observation, assume the earliest observed ticker(s)
    # go back to the company's start, within the limits of `_backfill_start`
    earliest = min((i.start for i in intervals), default=None)
    if earliest is not None:
        backfill_from = _backfill_start(
            earliest, span[0], names or [], resumptions or []
        )
        intervals += [
            _Interval(
                cik,
                i.ticker,
                backfill_from,
                earliest,
                "backfill",
                i.exchange,
                current_owner=False,
                observations=i.observations,
                listed=listed,
            )
            for i in list(intervals)
            if i.start == earliest and backfill_from < earliest
        ]

    clipped = []
    for interval in intervals:
        bounds = _clip(interval.start, interval.end, *span)
        if bounds is not None:
            interval.start, interval.end = bounds
            clipped.append(interval)
    return clipped


def _segment_company(
    cik: int,
    span: tuple[date, date | None],
    intervals: list[_Interval],
    names: list[tuple[date, str]],
    default_source: str,
) -> list[dict]:
    """Splits a company's life into rows of constant (ticker, name): one row
    per ticker valid over a period, or a null-ticker row for periods with
    none, with consecutive identical rows merged."""
    start, end = span
    name_days = [d for d, _ in names]
    if end is not None and end <= start:
        # zero-length life (see `_company_span`): a single tickerless row
        return [
            {
                "cik": cik,
                "company_name": names[-1][1] if names else None,
                "ticker": None,
                "exchange": None,
                "valid_from": start,
                "valid_to": end,
                "source": default_source,
            }
        ]

    def name_at(day: date) -> str | None:
        if not names:
            return None
        return names[max(bisect_right(name_days, day) - 1, 0)][1]

    cuts = {start}
    for interval in intervals:
        cuts.add(interval.start)
        if interval.end is not None:
            cuts.add(interval.end)
    cuts.update(d for d in name_days if d > start)
    cuts = sorted(d for d in cuts if d >= start and (end is None or d < end))
    bounds = list(zip(cuts, cuts[1:] + [end]))

    rows: list[dict] = []
    open_rows: dict[tuple, dict] = {}
    for seg_start, seg_end in bounds:
        name = name_at(seg_start)
        active = [
            i
            for i in intervals
            if i.start <= seg_start and (i.end is None or seg_start < i.end)
        ]
        keys = (
            [(i.ticker, i.exchange, i.source) for i in active]
            if active
            else [(None, None, default_source)]
        )
        next_open = {}
        for ticker, exchange, source in keys:
            key = (ticker, exchange, source, name)
            row = open_rows.get(key)
            if row is not None and row["valid_to"] == seg_start:
                row["valid_to"] = seg_end
            else:
                row = {
                    "cik": cik,
                    "company_name": name,
                    "ticker": ticker,
                    "exchange": exchange,
                    "valid_from": seg_start,
                    "valid_to": seg_end,
                    "source": source,
                }
                rows.append(row)
            next_open[key] = row
        open_rows = next_open
    return rows


def _filename_events(
    xbrl_filings: pl.LazyFrame | None,
    declared_by_accession: dict[str, set[str]],
    declared_by_cik: dict[int, dict[date, set[str]]],
    known_keys: dict[int, dict[str, str]],
    observations: dict[int, dict[str, int]],
    equity_years: dict[int, dict[int, int]] | None = None,
    rejected: dict[tuple[int, str], str] | None = None,
    evidence_floor=None,
    coregistrants: pl.DataFrame | None = None,
) -> dict[int, dict[date, set[str]]]:
    """Tickers read from the file names of filings whose cover page declares
    none (see `_filename_prefixes`), per company and filing date. A prefix
    that is one of the company's declared or current tickers is always
    kept; otherwise it is dropped when:

    - it is shown to be a mnemonic: the company used it on filings declaring
      other tickers (e.g. `psx` for Phillips 66 Partners). If those filings
      always declared one same ticker, the prefix is an alias of it (`pseg`
      for PEG) and the company's earlier filings with it show that ticker,
      back to `evidence_floor(cik, day)` (its last change of name or long
      silence, see `_backfill_start`)
    - the company no longer had publicly held shares: its equity evidence
      (`EQUITY_FORMS`) stopped more than `EQUITY_LAPSE_YEARS` earlier. Such
      a report is kept as one showing no ticker, ending any still open
    - another company used it at the same time (a subsidiary filing under
      its parent's ticker): over the common period it goes to the one with
      more equity evidence, to neither on a tie
    - it appears on fewer than `MIN_FILENAME_OBSERVATIONS` filings and the
      company shows no equity evidence around it

    `rejected`, if given, collects the reason each dropped (cik, prefix) was
    dropped, for diagnostics."""
    equity_years = equity_years or {}
    rejected = {} if rejected is None else rejected
    if xbrl_filings is None:
        return {}
    prefixes = _filename_prefixes(xbrl_filings, coregistrants)

    declared_keys: dict[int, set[str]] = defaultdict(set)
    for cik, days in declared_by_cik.items():
        for tickers in days.values():
            declared_keys[cik].update(_ticker_key(t) for t in tickers)
    for cik, keys in known_keys.items():
        declared_keys[cik].update(keys)

    contradicted: set[tuple[int, str]] = set()
    cooccurring: dict[tuple[int, str], list[set[str]]] = defaultdict(list)
    first_cooccurrence: dict[tuple[int, str], date] = {}
    undeclared = []
    for cik, accession, filed, prefix in prefixes.iter_rows():
        declared = declared_by_accession.get(accession)
        if declared:
            if prefix not in declared:
                contradicted.add((cik, prefix))
            cooccurring[(cik, prefix)].append(declared)
            first = first_cooccurrence.get((cik, prefix))
            first_cooccurrence[(cik, prefix)] = (
                filed if first is None else min(first, filed)
            )
        else:
            undeclared.append((cik, filed, prefix))

    # a contradiction only holds back to the company's last change of name
    # (or long silence): before it, the prefix may well have been its
    # ticker (DSW Inc. filed as `dsw` while trading as DSW, and still did
    # once renamed Designer Brands and trading as DBI)
    floors: dict[tuple[int, str], date] = {}
    # contradicted prefixes always seen with one same declared ticker
    aliases: dict[tuple[int, str], tuple[str, date, date]] = {}
    for cik, prefix in contradicted:
        first = first_cooccurrence[(cik, prefix)]
        floor = evidence_floor(cik, first) if evidence_floor else date.min
        floors[(cik, prefix)] = floor
        common = set.intersection(*cooccurring[(cik, prefix)])
        if len(common) == 1 and prefix not in declared_keys[cik]:
            aliases[(cik, prefix)] = (common.pop(), floor, first)

    spelling: dict[int, dict[str, str]] = defaultdict(dict)
    for cik, days in declared_by_cik.items():
        for tickers in days.values():
            for t in tickers:
                spelling[cik].setdefault(_ticker_key(t), t)
    for cik, keys in known_keys.items():
        spelling[cik].update(keys)

    last_declared = {cik: max(days) for cik, days in declared_by_cik.items() if days}

    def equity_around(cik: int, start: date, end: date) -> int:
        years = equity_years.get(cik, {})
        return sum(n for y, n in years.items() if start.year - 1 <= y <= end.year + 1)

    def equity_lapsed(cik: int, filed: date) -> bool:
        if cik in known_keys or last_declared.get(cik, date.min) > filed:
            return False  # still listed later on
        return _equity_lapsed(equity_years.get(cik, {}), filed)

    candidates = []
    lapsed_days: dict[int, set[date]] = defaultdict(set)
    for cik, filed, prefix in undeclared:
        if prefix in declared_keys[cik]:
            candidates.append((cik, filed, prefix))
        elif (cik, prefix) in contradicted and filed >= floors[(cik, prefix)]:
            alias = aliases.get((cik, prefix))
            if alias is not None and alias[1] <= filed < alias[2]:
                if equity_lapsed(cik, filed):
                    rejected.setdefault((cik, prefix), "equity lapsed")
                    lapsed_days[cik].add(filed)
                else:
                    candidates.append((cik, filed, alias[0]))
            else:
                rejected.setdefault((cik, prefix), "mnemonic")
        elif equity_lapsed(cik, filed):
            rejected.setdefault((cik, prefix), "equity lapsed")
            lapsed_days[cik].add(filed)
        else:
            candidates.append((cik, filed, prefix))

    # simultaneous use of a prefix by several companies: over the period
    # they overlap, it goes to the one with more equity evidence
    used: dict[str, dict[int, list[date]]] = defaultdict(lambda: defaultdict(list))
    for cik, filed, prefix in candidates:
        used[prefix][cik].append(filed)
    lost: dict[tuple[int, str], list[tuple[date, date]]] = defaultdict(list)
    for prefix, by_cik in used.items():
        if len(by_cik) < 2:
            continue
        ranges = {c: (min(d), max(d)) for c, d in by_cik.items()}
        verified = {c for c in ranges if prefix in declared_keys[c]}
        for c1 in ranges:
            for c2 in ranges:
                if c1 >= c2:
                    continue
                start = max(ranges[c1][0], ranges[c2][0])
                end = min(ranges[c1][1], ranges[c2][1])
                if end - start < SIMULTANEOUS_USE:
                    continue  # one took over from the other
                if verified & {c1, c2}:
                    winners = verified & {c1, c2}
                else:
                    e1, e2 = equity_around(c1, start, end), equity_around(
                        c2, start, end
                    )
                    winners = {c1} if e1 > e2 else {c2} if e2 > e1 else set()
                for loser in {c1, c2} - winners if len(winners) == 1 else {c1, c2}:
                    lost[(loser, prefix)].append((start, end))

    kept = []
    for cik, filed, prefix in candidates:
        if any(s <= filed <= e for s, e in lost.get((cik, prefix), [])):
            rejected.setdefault((cik, prefix), "shared")
        else:
            kept.append((cik, filed, prefix))
    counts: dict[tuple[int, str], int] = defaultdict(int)
    for cik, _, prefix in kept:
        counts[(cik, prefix)] += 1

    events: dict[int, dict[date, set[str]]] = defaultdict(lambda: defaultdict(set))
    for cik, filed, prefix in sorted(kept, key=lambda r: r[1]):
        if (
            counts[(cik, prefix)] < MIN_FILENAME_OBSERVATIONS
            and prefix not in declared_keys[cik]
            and not equity_around(cik, filed, filed)
        ):
            rejected.setdefault((cik, prefix), "single filing")
            continue
        rejected.pop((cik, prefix), None)
        ticker = spelling[cik].get(prefix, prefix)
        events[cik][filed].add(ticker)
        observations[cik][ticker] += 1
    # a report filed once the shares were no longer public shows no ticker:
    # it ends any ticker still open
    for cik, days in lapsed_days.items():
        for day in days:
            if day not in events[cik]:
                events[cik][day] = set()
    return events


def _text_events(
    text_tickers: pl.LazyFrame | None,
) -> dict[int, list[tuple[date, frozenset[str], str]]]:
    """cik -> [(report date, {symbol}, "filing_text")] from the symbols
    found in annual reports (`SecFilingTextTickers`). Candidates are cleaned
    (`clean_symbol`) and counted again, and a report whose two most
    mentioned symbols are tied names none."""
    if text_tickers is None:
        return {}
    columns = text_tickers.collect_schema().names()
    rows = (
        text_tickers.filter(pl.col("symbol").is_not_null())
        .select(
            "cik",
            "date_filed",
            "symbol",
            pl.col("candidates") if "candidates" in columns else pl.lit(None),
        )
        .collect()
    )
    events: dict[int, list] = defaultdict(list)
    for cik, filed, symbol, candidates in rows.iter_rows():
        counts: dict[str, int] = defaultdict(int)
        for candidate in (candidates or f"{symbol}:1").split(","):
            raw, _, n = candidate.rpartition(":")
            cleaned = clean_symbol(raw)
            if cleaned and _TICKER_RE.match(cleaned):
                counts[cleaned] += int(n)
        ranked = sorted(counts.values(), reverse=True)
        if not ranked or (len(ranked) > 1 and ranked[0] == ranked[1]):
            continue
        best = max(counts, key=counts.get)
        events[cik].append((filed, frozenset([best]), "filing_text"))
    return events


def compute_security_master(
    filings: pl.LazyFrame,
    cover_pages: pl.LazyFrame,
    current: pl.DataFrame,
    snapshot_date: date,
    xbrl_filings: pl.LazyFrame | None = None,
    text_tickers: pl.LazyFrame | None = None,
) -> pl.DataFrame:
    """Builds the security master (see module docstring) from:

    - `filings`: the EDGAR full-index (`cik`, `company_name`, `form_type`,
      `date_filed`)
    - `cover_pages`: XBRL cover page facts as stored by `SecXbrlCoverPages`
    - `current`: today's mapping (`cik`, `ticker`, `company_name`,
      `exchange`), as of `snapshot_date`
    - `xbrl_filings`: XBRL filings with their file names, as stored by
      `SecXbrlFilings` (optional: without it tickers only come from cover
      pages and today's mapping)
    - `text_tickers`: the symbol named in annual reports of companies that
      never filed XBRL, as stored by `SecFilingTextTickers` (optional); used
      only for companies with no other observed ticker
    """
    coregistrants = _coregistrants(filings)
    filings = filings.select("cik", "company_name", "form_type", "date_filed")
    by_text = _text_events(text_tickers)
    lifecycles = _company_lifecycles(filings)
    declarations = _ticker_declarations(cover_pages)

    # today's tickers per company, by spelling-insensitive key
    snapshot: dict[int, dict[str, str | None]] = defaultdict(dict)
    current_names: dict[int, str] = {}
    for row in current.iter_rows(named=True):
        snapshot[row["cik"]][row["ticker"]] = row.get("exchange")
        current_names.setdefault(row["cik"], row.get("company_name"))
    known_keys = {
        cik: {_ticker_key(t): t for t in tickers} for cik, tickers in snapshot.items()
    }

    # declared tickers, grouped per company and filing date
    by_filing: dict[int, dict[date, set[str]]] = defaultdict(lambda: defaultdict(set))
    declared_by_accession: dict[str, set[str]] = defaultdict(set)
    observations: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    exchanges: dict[int, dict[str, str]] = defaultdict(dict)
    equity_years = _equity_years(filings)
    for cik, adsh, filed, raw, exchange in declarations.sort("filed").iter_rows():
        if (
            filed < MANDATORY_COVER_PAGE_DATE
            and not exchange
            and cik not in snapshot
            and _equity_lapsed(equity_years.get(cik, {}), filed)
        ):
            # an optional pre-2019 declaration by a company no longer public
            continue
        for ticker in normalize_tickers(raw, known_keys.get(cik)):
            by_filing[cik][filed].add(ticker)
            declared_by_accession[adsh].add(_ticker_key(ticker))
            observations[cik][ticker] += 1
            if exchange:
                exchanges[cik][ticker] = exchange

    # the companies in the master: anyone who filed periodic reports,
    # declared a ticker, or is in today's mapping
    periodic_ciks = lifecycles.filter(pl.col("first_filing_date").is_not_null())["cik"]
    ciks = sorted(set(periodic_ciks.to_list()) | set(by_filing) | set(snapshot))
    resumptions: dict[int, list[date]] = defaultdict(list)
    for cik, resumed in (
        _reporting_resumptions(filings).sort("cik", "resumed").iter_rows()
    ):
        resumptions[cik].append(resumed)
    ciks_df = pl.DataFrame({"cik": ciks}, schema={"cik": pl.Int64})
    names_df = _name_history(filings, ciks_df).sort("cik", "since")
    names: dict[int, list[tuple[date, str]]] = defaultdict(list)
    for cik, since, name in names_df.iter_rows():
        names[cik].append((since, name))

    # tickers from file names, for filings declaring none on their cover
    by_filename = _filename_events(
        xbrl_filings,
        declared_by_accession,
        by_filing,
        known_keys,
        observations,
        equity_years=equity_years,
        evidence_floor=lambda cik, day: _backfill_start(
            day, date.min, names.get(cik, []), resumptions.get(cik, [])
        ),
        coregistrants=coregistrants,
    )

    lifecycle_by_cik = {
        row["cik"]: row
        for row in lifecycles.join(ciks_df, on="cik", how="semi").iter_rows(named=True)
    }

    intervals: list[_Interval] = []
    spans: dict[int, tuple[date, date | None]] = {}
    company_columns: dict[int, dict] = {}
    for cik in ciks:
        life = dict(lifecycle_by_cik.get(cik, {}))
        declared_days = by_filing.get(cik, {})
        events = sorted(
            [(day, frozenset(ts), "xbrl") for day, ts in declared_days.items()]
            + [
                (day, frozenset(ts), "xbrl_filename")
                for day, ts in by_filename.get(cik, {}).items()
                if day not in declared_days
            ]
        )
        if not any(ts for _, ts, _ in events) and cik in by_text:
            events = sorted(events + by_text[cik])
        declared_events = [e for e in events if e[2] == "xbrl"]
        is_current = cik in snapshot
        # a company in today's mapping is listed: past Form 25/15s were
        # about single classes or exchange transfers
        last_report = life.get("last_filing_date")
        life["delisting_date"] = (
            None
            if is_current
            else _terminal_delisting(
                life.get("delistings"),
                last_report,
                _reporting_grace(life.get("last_periodic_form")),
            )
        )
        life["deregistration_date"] = (
            None
            if is_current
            else _terminal_deregistration(life.get("deregistrations"), last_report)
        )
        life["last_declaration"] = declared_events[-1][0] if declared_events else None
        span = _company_span(
            life,
            declared_events[0][0] if declared_events else None,
            is_current,
            snapshot_date,
        )
        spans[cik] = span
        company_columns[cik] = {
            "first_filing_date": life.get("first_filing_date"),
            "last_filing_date": life.get("last_filing_date"),
            "delisting_date": life["delisting_date"],
            "deregistration_date": life["deregistration_date"],
        }
        if not names[cik] and cik in current_names:
            names[cik].append((span[0], current_names[cik]))
        intervals.extend(
            _ticker_intervals(
                cik,
                events,
                observations.get(cik, {}),
                {
                    **{t: e for t, e in exchanges.get(cik, {}).items()},
                    **{t: e for t, e in snapshot.get(cik, {}).items() if e},
                },
                snapshot.get(cik) if is_current else None,
                snapshot_date,
                span,
                names=names[cik],
                resumptions=resumptions.get(cik),
                listed=bool(life.get("listed"))
                or is_current
                or bool(exchanges.get(cik)),
            )
        )

    per_company: dict[int, list[_Interval]] = defaultdict(list)
    for interval in _resolve_ticker_conflicts(intervals):
        per_company[interval.cik].append(interval)

    rows = []
    for cik in ciks:
        default_source = "full-index" if cik in lifecycle_by_cik else "company_tickers"
        company_rows = _segment_company(
            cik, spans[cik], per_company.get(cik, []), names[cik], default_source
        )
        has_ticker = any(r["ticker"] is not None for r in company_rows)
        for row in company_rows:
            row.update(company_columns[cik])
            row["delisting_reason"] = None
            row["has_known_ticker"] = has_ticker
            rows.append(row)

    return pl.DataFrame(rows, schema=_SCHEMA).sort("cik", "valid_from", "ticker")


# --- quality checks -----------------------------------------------------------


def current_tickers_still_valid(
    lf: pl.LazyFrame, dataplatform_root: str
) -> pl.LazyFrame:
    """Every (cik, ticker) in today's `company_tickers.json` must have an
    open-ended row."""
    open_rows = lf.filter(pl.col("valid_to").is_null()).select("cik", "ticker")
    return (
        CompanyTickersSilver(dataplatform_root=dataplatform_root)
        .read_from_disk()
        .select(pl.col("cik_str").alias("cik"), "ticker")
        .join(open_rows, on=["cik", "ticker"], how="anti")
    )


def rows_end_by_terminal_filings(lf: pl.LazyFrame) -> pl.LazyFrame:
    """A company delisted (Form 25) or deregistered (Form 15) has no row
    valid after that date."""
    end = pl.min_horizontal("delisting_date", "deregistration_date")
    return lf.filter(
        end.is_not_null() & (pl.col("valid_to").is_null() | (pl.col("valid_to") > end))
    )


def tickers_are_not_placeholders(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Tickers contain a letter and aren't placeholder words (cover pages
    sometimes declare `N/A`, `None`, `TRUE`, `0`)."""
    return lf.filter(
        pl.col("ticker").is_not_null()
        & (
            ~pl.col("ticker").str.contains("[A-Z]")
            | pl.col("ticker").is_in(sorted(_NOT_A_TICKER))
        )
    )


def backfill_extends_an_observed_ticker(lf: pl.LazyFrame) -> pl.LazyFrame:
    """A backfilled row only extends backwards a ticker the same company was
    later observed with (declared, read from a file name or current), under
    the same company name (see `company_name_key`)."""
    name_key = pl.col("company_name").map_elements(
        company_name_key, return_dtype=pl.String
    )
    observed = (
        lf.filter(pl.col("ticker").is_not_null(), pl.col("source") != "backfill")
        .group_by("cik", "ticker")
        .agg(
            pl.col("valid_from").min().alias("first_observed"),
            pl.col("company_name").sort_by("valid_from").first().alias("observed_name"),
        )
    )
    return (
        lf.filter(pl.col("source") == "backfill")
        .join(observed, on=["cik", "ticker"], how="left")
        .filter(
            pl.col("first_observed").is_null()
            | (pl.col("valid_to") > pl.col("first_observed"))
            | (
                name_key
                != pl.col("observed_name").map_elements(
                    company_name_key, return_dtype=pl.String
                )
            )
        )
    )


# companies delisted from an exchange (terminal Form 25) after every filer
# had to file XBRL (mid-2011): their ticker comes from cover pages or XBRL
# file names, and only a few have none (BDCs, exempt from XBRL until 2022;
# foreign issuers without equity evidence and a single report)
XBRL_ERA_START = date(2011, 7, 1)
XBRL_ERA_DELISTED_WITH_TICKER = 0.93


def tickerless_rows_do_not_overlap_company_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    """A null-ticker row means "no known ticker in this period": it must not
    overlap any other row of the same company."""
    rows = lf.select("cik", "ticker", "valid_from", "valid_to").with_row_index("_row")
    tickerless = rows.filter(pl.col("ticker").is_null())
    return (
        tickerless.join(rows, on="cik", suffix="_other")
        .filter(pl.col("_row") != pl.col("_row_other"))
        .filter(
            (
                pl.col("valid_to_other").is_null()
                | (pl.col("valid_from") < pl.col("valid_to_other"))
            )
            & (
                pl.col("valid_to").is_null()
                | (pl.col("valid_from_other") < pl.col("valid_to"))
            )
        )
        .drop("_row", "_row_other")
    )


class SecurityMasterSilver(Model):
    def __init__(self, dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT) -> None:
        super().__init__(
            name="security_master",
            layer="silver",
            quality_checks=[
                not_empty(),
                not_null(["cik", "valid_from", "source", "has_known_ticker"]),
                accepted_values("source", SOURCES),
                matches_regex("ticker", TICKER_PATTERN),
                comparison("valid_from", "<=", "valid_to"),
                unique(["cik", "ticker", "valid_from"]),
                # a ticker belongs to a single company at a time
                no_overlapping_intervals("ticker", distinct_by="cik"),
                no_overlapping_intervals(["cik", "ticker"]),
                tickerless_rows_do_not_overlap_company_rows,
                with_root(current_tickers_still_valid, dataplatform_root),
                rows_end_by_terminal_filings,
                tickers_are_not_placeholders,
                backfill_extends_an_observed_ticker,
                min_share(
                    "xbrl_era_delisted_companies_with_ticker",
                    pl.col("has_known_ticker"),
                    XBRL_ERA_DELISTED_WITH_TICKER,
                    among=pl.col("delisting_date").is_not_null()
                    & (pl.col("last_filing_date") >= XBRL_ERA_START),
                    unique_by="cik",
                ),
            ],
            dataplatform_root=dataplatform_root,
        )

    def _snapshot(self) -> tuple[pl.DataFrame, date]:
        tickers = (
            CompanyTickersSilver(dataplatform_root=self.dataplatform_root)
            .read_from_disk()
            .select(
                pl.col("cik_str").alias("cik"),
                "ticker",
                pl.col("title").alias("company_name"),
            )
            .collect()
        )
        exchange_source = SecTickersExchange(dataplatform_root=self.dataplatform_root)
        payload = exchange_source.read_from_disk()[0]
        exchanges = pl.DataFrame(
            payload["data"], schema=payload["fields"], orient="row"
        ).select(
            pl.col("cik").cast(pl.Int64),
            "ticker",
            _canonical_exchange(pl.col("exchange")).alias("exchange"),
        )
        snapshot_file = SecTickers(dataplatform_root=self.dataplatform_root).root_dir
        snapshot_date = datetime.fromtimestamp(
            (snapshot_file / "company_tickers.json").stat().st_mtime
        ).date()
        return (
            tickers.join(exchanges, on=["cik", "ticker"], how="left"),
            snapshot_date,
        )

    def _build(self) -> pl.LazyFrame:
        current, snapshot_date = self._snapshot()
        return compute_security_master(
            SecFullIndex(dataplatform_root=self.dataplatform_root).read_from_disk(),
            SecXbrlCoverPages(
                dataplatform_root=self.dataplatform_root
            ).read_from_disk(),
            current,
            snapshot_date,
            xbrl_filings=SecXbrlFilings(
                dataplatform_root=self.dataplatform_root
            ).read_from_disk(),
            text_tickers=self._text_tickers(),
        ).lazy()

    def _text_tickers(self) -> pl.LazyFrame | None:
        source = SecFilingTextTickers(dataplatform_root=self.dataplatform_root)
        if not any(source.root_dir.glob("*.parquet")):
            return None
        return source.read_from_disk()


# --- helpers for downstream consumers -------------------------------------------


def _master(
    master: pl.LazyFrame | pl.DataFrame | None, dataplatform_root: str
) -> pl.LazyFrame:
    if master is None:
        return SecurityMasterSilver(
            dataplatform_root=dataplatform_root
        ).read_from_disk()
    return master.lazy()


def _valid_on(day: date) -> pl.Expr:
    return (pl.col("valid_from") <= day) & (
        pl.col("valid_to").is_null() | (pl.lit(day) < pl.col("valid_to"))
    )


def listed_at(
    day: date,
    require_ticker: bool = False,
    include_backfill: bool = True,
    master: pl.LazyFrame | pl.DataFrame | None = None,
    dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
) -> pl.DataFrame:
    """The security master rows valid on `day`: one per (company, ticker)
    alive that day, plus one null-ticker row for companies alive with no
    known ticker (unless `require_ticker`). `include_backfill=False` drops
    tickers that are only assumed (before their first declaration).

    For a symbol universe (e.g. `MarketView.universe()`) use
    `require_ticker=True` and the `ticker` column; `cik` stays the stable
    identity across ticker changes."""
    rows = _master(master, dataplatform_root).filter(_valid_on(day))
    if require_ticker:
        rows = rows.filter(pl.col("ticker").is_not_null())
    if not include_backfill:
        rows = rows.filter(pl.col("source") != "backfill")
    return rows.collect()


def ticker_to_cik(
    ticker: str,
    day: date,
    include_backfill: bool = True,
    master: pl.LazyFrame | pl.DataFrame | None = None,
    dataplatform_root: str = DEFAULT_DATAPLATFORM_ROOT,
) -> int | None:
    """The CIK of the company trading as `ticker` on `day`, or None."""
    rows = _master(master, dataplatform_root).filter(
        (pl.col("ticker") == ticker) & _valid_on(day)
    )
    if not include_backfill:
        rows = rows.filter(pl.col("source") != "backfill")
    ciks = rows.select("cik").unique().collect()["cik"].to_list()
    return ciks[0] if ciks else None
