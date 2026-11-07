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
  XBRL cover page (2009 onwards, mandatory since 2019) and from today's
  `company_tickers.json`. A ticker is valid from the first filing declaring
  it until the first later filing of the same company that no longer does.
  Before the first declaration the earliest known ticker is assumed to go
  back to the company's start (`source = "backfill"`).
- **Conflicts**: a ticker belongs to one company at a time. When two
  companies claim it over the same period, today's owner wins, then
  declared beats backfilled, then the company declaring it more often.
"""

import re
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from etl.ingestion.sec import (
    SecFullIndex,
    SecTickers,
    SecTickersExchange,
    SecXbrlCoverPages,
)
from etl.logger import get_logger
from etl.transformation.model import DEFAULT_DATAPLATFORM_ROOT, Model
from etl.transformation.quality_checks import (
    accepted_values,
    comparison,
    matches_regex,
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

SOURCES = ["full-index", "xbrl", "company_tickers", "backfill"]
TICKER_PATTERN = r"[A-Z0-9]{1,6}([.\-][A-Z0-9]{1,4})?"
_TICKER_RE = re.compile(f"^{TICKER_PATTERN}$")
_NOT_A_TICKER = {"NONE", "NA", "N/A", "N.A.", "NOTAPPLICABLE", "-", "NIL", "TBD"}

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
        )
        .collect(engine="streaming")
    )


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
        if key in known:
            ticker = known[key]
        else:
            ticker = re.sub(r"[\s./]+", "-", piece)
        if _TICKER_RE.match(ticker) and ticker not in tickers:
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

    def priority(self) -> tuple:
        """Sort key: lower wins a conflict over the same ticker."""
        return (
            not self.current_owner,
            self.source == "backfill",
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


def _resolve_ticker_conflicts(intervals: list[_Interval]) -> list[_Interval]:
    """Makes each ticker belong to at most one company at a time: intervals
    are granted in priority order (see `_Interval.priority`), each keeping
    only the parts not already granted to another company."""
    by_ticker: dict[str, list[_Interval]] = defaultdict(list)
    for interval in intervals:
        by_ticker[interval.ticker].append(interval)

    resolved = []
    for claims in by_ticker.values():
        claims.sort(key=_Interval.priority)
        granted: list[_Interval] = []
        for claim in claims:
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
        if event is not None and event > start
    ]
    if terminal_events:
        return start, min(terminal_events)

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


def _ticker_intervals(
    cik: int,
    events: list[tuple[date, frozenset[str]]],
    observations: dict[str, int],
    exchanges: dict[str, str | None],
    snapshot: dict[str, str | None] | None,
    snapshot_date: date,
    span: tuple[date, date | None],
) -> list[_Interval]:
    """Validity intervals of each ticker a company declared (`events`: one
    (filing date, tickers) per filing date, sorted) or currently has
    (`snapshot`: ticker -> exchange, None if the company isn't current)."""
    seen: dict[str, list[date]] = defaultdict(list)
    for day, tickers in events:
        for ticker in tickers:
            seen[ticker].append(day)

    intervals = []
    for ticker, days in seen.items():
        first, last = days[0], days[-1]
        current_owner = snapshot is not None and ticker in snapshot
        if current_owner:
            # still the company's ticker today, whatever filings in between
            # omitted (or mangled) it
            end = None
        else:
            end = next((d for d, ts in events if d > last and ticker not in ts), None)
            if end is None and snapshot is not None and snapshot_date > last:
                # the company is current but today's mapping no longer lists it
                end = snapshot_date
        intervals.append(
            _Interval(
                cik,
                ticker,
                first,
                end,
                "xbrl",
                exchanges.get(ticker),
                current_owner=current_owner,
                observations=len(days),
            )
        )

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
            )
        )

    # before its first declaration, assume the earliest known ticker(s) go
    # back to the company's start
    earliest = min((i.start for i in intervals), default=None)
    backfills = [
        _Interval(
            cik,
            i.ticker,
            span[0],
            earliest,
            "backfill",
            i.exchange,
            current_owner=False,
            observations=i.observations,
        )
        for i in intervals
        if i.start == earliest and span[0] < earliest
    ]

    clipped = []
    for interval in intervals + backfills:
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


def compute_security_master(
    filings: pl.LazyFrame,
    cover_pages: pl.LazyFrame,
    current: pl.DataFrame,
    snapshot_date: date,
) -> pl.DataFrame:
    """Builds the security master (see module docstring) from:

    - `filings`: the EDGAR full-index (`cik`, `company_name`, `form_type`,
      `date_filed`)
    - `cover_pages`: XBRL cover page facts as stored by `SecXbrlCoverPages`
    - `current`: today's mapping (`cik`, `ticker`, `company_name`,
      `exchange`), as of `snapshot_date`
    """
    filings = filings.select("cik", "company_name", "form_type", "date_filed")
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
    observations: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    exchanges: dict[int, dict[str, str]] = defaultdict(dict)
    for cik, _, filed, raw, exchange in declarations.sort("filed").iter_rows():
        for ticker in normalize_tickers(raw, known_keys.get(cik)):
            by_filing[cik][filed].add(ticker)
            observations[cik][ticker] += 1
            if exchange:
                exchanges[cik][ticker] = exchange

    # the companies in the master: anyone who filed periodic reports,
    # declared a ticker, or is in today's mapping
    periodic_ciks = lifecycles.filter(pl.col("first_filing_date").is_not_null())["cik"]
    ciks = sorted(set(periodic_ciks.to_list()) | set(by_filing) | set(snapshot))
    ciks_df = pl.DataFrame({"cik": ciks}, schema={"cik": pl.Int64})
    names_df = _name_history(filings, ciks_df).sort("cik", "since")
    names: dict[int, list[tuple[date, str]]] = defaultdict(list)
    for cik, since, name in names_df.iter_rows():
        names[cik].append((since, name))

    lifecycle_by_cik = {
        row["cik"]: row
        for row in lifecycles.join(ciks_df, on="cik", how="semi").iter_rows(named=True)
    }

    intervals: list[_Interval] = []
    spans: dict[int, tuple[date, date | None]] = {}
    company_columns: dict[int, dict] = {}
    for cik in ciks:
        life = dict(lifecycle_by_cik.get(cik, {}))
        events = sorted(
            (day, frozenset(tickers)) for day, tickers in by_filing.get(cik, {}).items()
        )
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
        life["last_declaration"] = events[-1][0] if events else None
        span = _company_span(
            life, events[0][0] if events else None, is_current, snapshot_date
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
        ).lazy()


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
