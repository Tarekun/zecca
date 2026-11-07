import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.quality_checks import no_overlapping_intervals
from etl.transformation.silver.security_master import (
    QUARTERLY_REPORTER_GRACE,
    compute_security_master,
    listed_at,
    normalize_tickers,
    ticker_to_cik,
    tickerless_rows_do_not_overlap_company_rows,
)

SNAPSHOT = date(2026, 11, 7)

META, ALPHABET, TWITTER, LEHMAN = 1326801, 1652044, 1418091, 806085
OLD_ABC, NEW_ABC, SUBSIDIARY, TRANSFERRED, GONE_DARK = 1, 2, 3, 4, 5


# --- synthetic inputs ---


def _quarterly(start: date, end: date) -> list[date]:
    days, day = [], start
    while day <= end:
        days.append(day)
        day += timedelta(days=91)
    return days


def _filings(cik, name, form, days):
    return [
        {"cik": cik, "company_name": name, "form_type": form, "date_filed": d}
        for d in days
    ]


def _declarations(cik, days, *tickers, exchange="NASDAQ", coreg=""):
    rows = []
    for d in days:
        adsh = f"{cik}-{d.isoformat()}"
        for i, ticker in enumerate(tickers):
            dimh = f"0x{i}"
            base = {
                "adsh": adsh,
                "cik": cik,
                "company_name": "",
                "form": "10-Q",
                "period": "",
                "filed": d,
                "ddate": "",
                "dimh": dimh,
                "coreg": coreg,
                "context": f"c-{i}",
            }
            rows.append({**base, "tag": "TradingSymbol", "value": ticker})
            rows.append({**base, "tag": "SecurityExchangeName", "value": exchange})
    return rows


def _inputs():
    filings, cover = [], []

    # FB -> META: same CIK, renamed in 2021, ticker changed in 2022
    meta_reports = _quarterly(date(2012, 7, 31), date(2026, 10, 30))
    filings += _filings(
        META,
        "Facebook Inc",
        "10-Q",
        [d for d in meta_reports if d < date(2021, 10, 28)],
    )
    filings += _filings(
        META,
        "Meta Platforms, Inc.",
        "10-Q",
        [d for d in meta_reports if d >= date(2021, 10, 28)],
    )
    cover += _declarations(
        META,
        [d for d in meta_reports if date(2019, 1, 1) <= d < date(2022, 6, 1)],
        "FB",
    )
    cover += _declarations(
        META, [d for d in meta_reports if d >= date(2022, 6, 1)], "META"
    )

    # GOOG/GOOGL: two classes at the same time
    alphabet_reports = _quarterly(date(2015, 11, 1), date(2026, 10, 30))
    filings += _filings(ALPHABET, "Alphabet Inc.", "10-Q", alphabet_reports)
    # successor of Google Inc. (holding company reorganization)
    filings += _filings(ALPHABET, "Alphabet Inc.", "8-K12B", [date(2015, 10, 2)])
    # a class of notes delisted: Alphabet is still listed
    filings += _filings(ALPHABET, "Alphabet Inc.", "25-NSE", [date(2025, 11, 14)])
    cover += _declarations(
        ALPHABET, [d for d in alphabet_reports if d.year >= 2019], "GOOGL", "GOOG"
    )
    # a later 8-K only lists the class A shares: GOOG is still current
    cover += _declarations(ALPHABET, [date(2026, 10, 31)], "GOOGL")

    # Twitter: acquired and delisted in 2022, not in today's mapping
    twitter_reports = _quarterly(date(2013, 11, 14), date(2022, 8, 2))
    filings += _filings(TWITTER, "TWITTER, INC.", "10-Q", twitter_reports)
    filings += _filings(TWITTER, "TWITTER, INC.", "25-NSE", [date(2022, 10, 28)])
    filings += _filings(TWITTER, "TWITTER, INC.", "15-12G", [date(2022, 11, 7)])
    cover += _declarations(
        TWITTER, [d for d in twitter_reports if d.year >= 2019], "TWTR", exchange="NYSE"
    )

    # Lehman Brothers: bankrupt in 2008, before XBRL (no ticker known)
    lehman_reports = _quarterly(date(1994, 1, 14), date(2008, 7, 10))
    filings += _filings(LEHMAN, "LEHMAN BROTHERS HOLDINGS INC", "10-Q", lehman_reports)
    # notes delisted while still reporting and right after the last report,
    # then the stock, then more notes long after
    filings += _filings(
        LEHMAN,
        "LEHMAN BROTHERS HOLDINGS INC",
        "25-NSE",
        [date(2002, 1, 7), date(2008, 7, 25), date(2008, 9, 17), date(2009, 3, 18)],
    )
    filings += _filings(
        LEHMAN, "LEHMAN BROTHERS HOLDINGS INC PLAN TRUST", "15-12B", [date(2012, 3, 12)]
    )
    # Lehman's later non-periodic filings don't keep it alive
    filings += _filings(
        LEHMAN,
        "LEHMAN BROTHERS HOLDINGS INC PLAN TRUST",
        "8-K",
        [date(2010, 1, 1), date(2015, 1, 1)],
    )

    # ticker ABC reassigned: old company delisted in 2020, new one from 2021
    old_reports = _quarterly(date(2015, 1, 1), date(2020, 3, 1))
    filings += _filings(OLD_ABC, "OLD ABC CORP", "10-Q", old_reports)
    filings += _filings(OLD_ABC, "OLD ABC CORP", "25-NSE", [date(2020, 6, 1)])
    cover += _declarations(OLD_ABC, [d for d in old_reports if d.year >= 2019], "ABC")
    new_reports = _quarterly(date(2021, 2, 1), date(2026, 10, 1))
    filings += _filings(NEW_ABC, "NEW ABC INC", "8-A12B", [date(2021, 1, 15)])
    filings += _filings(NEW_ABC, "NEW ABC INC", "10-Q", new_reports)
    cover += _declarations(NEW_ABC, new_reports, "ABC")

    # a subsidiary declaring its parent's ticker in its own filings
    filings += _filings(SUBSIDIARY, "ALPHABET SUB LLC", "10-Q", alphabet_reports)
    cover += _declarations(
        SUBSIDIARY, [d for d in alphabet_reports if d.year >= 2023], "GOOGL"
    )

    # moved to another exchange in 2015 (Form 25 by the old one) and alive
    transferred = _quarterly(date(2010, 1, 1), date(2026, 10, 1))
    filings += _filings(TRANSFERRED, "TRANSFERRED CO", "10-Q", transferred)
    filings += _filings(TRANSFERRED, "TRANSFERRED CO", "25-NSE", [date(2015, 5, 1)])

    # stopped filing in 2016 without any Form 25/15
    filings += _filings(
        GONE_DARK, "GONE DARK INC", "10-K", [date(2014, 3, 1), date(2015, 3, 1)]
    )
    filings += _filings(GONE_DARK, "GONE DARK INC", "10-Q", [date(2015, 11, 10)])

    # an individual filing ownership reports: not a company, not in the master
    filings += _filings(999, "SOMEONE JOHN", "4", [date(2020, 1, 1)])

    current = pl.DataFrame(
        {
            "cik": [META, META, ALPHABET, ALPHABET, NEW_ABC, TRANSFERRED],
            "ticker": ["META", "METAM", "GOOGL", "GOOG", "ABC", "TRNS"],
            "company_name": [
                "Meta Platforms, Inc.",
                "Meta Platforms, Inc.",
                "Alphabet Inc.",
                "Alphabet Inc.",
                "NEW ABC INC",
                "TRANSFERRED CO",
            ],
            "exchange": ["NASDAQ", "NASDAQ", "NASDAQ", "NASDAQ", "NYSE", "NYSE"],
        }
    )
    cover_schema = {c: pl.String for c in cover[0]} | {
        "cik": pl.Int64,
        "filed": pl.Date,
    }
    return (
        pl.DataFrame(filings).lazy(),
        pl.DataFrame(cover, schema=cover_schema).lazy(),
        current,
    )


@pytest.fixture(scope="module")
def master() -> pl.DataFrame:
    return compute_security_master(*_inputs(), snapshot_date=SNAPSHOT)


def _rows(master, cik, ticker=None):
    rows = master.filter(pl.col("cik") == cik)
    if ticker is not None:
        rows = rows.filter(pl.col("ticker") == ticker)
    return rows.sort("valid_from")


# --- known cases ---


def test_fb_to_meta_same_cik_consecutive_intervals(master):
    fb = _rows(master, META, "FB")
    meta = _rows(master, META, "META")

    assert fb["valid_to"].max() == meta["valid_from"].min()
    assert meta["valid_to"].is_null().all()
    assert ticker_to_cik("FB", date(2020, 6, 1), master=master) == META
    assert ticker_to_cik("META", date(2024, 6, 1), master=master) == META
    assert ticker_to_cik("FB", date(2024, 6, 1), master=master) is None


def test_fb_backfilled_to_ipo_and_renamed(master):
    fb = _rows(master, META, "FB")

    assert fb["valid_from"].min() == date(2012, 7, 31)
    first_declaration = min(
        d for d in _quarterly(date(2012, 7, 31), SNAPSHOT) if d >= date(2019, 1, 1)
    )
    assert (
        fb.filter(pl.col("source") == "backfill")["valid_to"].max() == first_declaration
    )
    assert (
        ticker_to_cik("FB", date(2015, 1, 1), master=master, include_backfill=False)
        is None
    )
    names = fb.sort("valid_from")["company_name"].unique(maintain_order=True).to_list()
    assert names == ["Facebook Inc", "Meta Platforms, Inc."]


def test_current_ticker_never_declared_starts_at_snapshot(master):
    metam = _rows(master, META, "METAM")

    assert metam.height == 1
    assert metam["valid_from"][0] == SNAPSHOT
    assert metam["source"][0] == "company_tickers"


def test_goog_googl_same_cik_simultaneous(master):
    day = date(2021, 6, 1)
    listed = listed_at(day, require_ticker=True, master=master)

    alphabet = listed.filter(pl.col("cik") == ALPHABET)["ticker"].sort().to_list()
    assert alphabet == ["GOOG", "GOOGL"]
    assert _rows(master, ALPHABET, "GOOG")["valid_to"][-1] is None


def test_successor_issuer_starts_at_its_8k12b(master):
    assert _rows(master, ALPHABET)["valid_from"].min() == date(2015, 10, 2)


def test_listed_company_has_no_delisting_date(master):
    assert _rows(master, ALPHABET)["delisting_date"].is_null().all()


def test_twitter_delisted_and_absent_from_current_mapping(master):
    twitter = _rows(master, TWITTER)

    assert twitter["delisting_date"][0] == date(2022, 10, 28)
    assert twitter["deregistration_date"][0] == date(2022, 11, 7)
    assert twitter["valid_to"].max() == date(2022, 10, 28)
    assert ticker_to_cik("TWTR", date(2021, 1, 4), master=master) == TWITTER
    assert TWITTER not in listed_at(date(2023, 1, 3), master=master)["cik"].to_list()


def test_lehman_bankrupt_without_ticker(master):
    lehman = _rows(master, LEHMAN)

    assert (
        lehman["last_filing_date"][0]
        == _quarterly(date(1994, 1, 14), date(2008, 7, 10))[-1]
    )
    assert lehman["delisting_date"][0] == date(2008, 9, 17)
    assert lehman["valid_to"].max() == date(2008, 9, 17)
    assert lehman["ticker"].is_null().all()
    assert not lehman["has_known_ticker"][0]
    assert LEHMAN in listed_at(date(2008, 1, 2), master=master)["cik"].to_list()


def test_reassigned_ticker_belongs_to_one_company_at_a_time(master):
    assert ticker_to_cik("ABC", date(2019, 12, 2), master=master) == OLD_ABC
    assert ticker_to_cik("ABC", date(2022, 1, 3), master=master) == NEW_ABC
    assert _rows(master, OLD_ABC, "ABC")["valid_to"].max() == date(2020, 6, 1)
    # the new company starts at its exchange registration, before any report
    assert _rows(master, NEW_ABC)["valid_from"].min() == date(2021, 1, 15)


def test_current_owner_wins_over_subsidiary_declaring_its_ticker(master):
    assert ticker_to_cik("GOOGL", date(2024, 1, 2), master=master) == ALPHABET
    assert _rows(master, SUBSIDIARY, "GOOGL").is_empty()


def test_exchange_transfer_is_not_a_delisting(master):
    transferred = _rows(master, TRANSFERRED)

    assert transferred["delisting_date"].is_null().all()
    assert transferred["valid_to"].is_null().any()
    assert TRANSFERRED in listed_at(date(2018, 1, 2), master=master)["cik"].to_list()


def test_company_gone_dark_ends_after_grace(master):
    gone = _rows(master, GONE_DARK)

    assert gone["valid_to"].max() == date(2015, 11, 10) + QUARTERLY_REPORTER_GRACE


def test_only_companies_in_master(master):
    assert 999 not in master["cik"].to_list()


def test_exchange_from_cover_page_and_snapshot(master):
    assert _rows(master, TWITTER, "TWTR")["exchange"].unique().to_list() == ["NYSE"]
    assert _rows(master, NEW_ABC, "ABC")["exchange"].unique().to_list() == ["NYSE"]


# --- invariants checked by the model's quality checks ---


@pytest.mark.parametrize(
    "check",
    [
        no_overlapping_intervals("ticker", distinct_by="cik"),
        no_overlapping_intervals(["cik", "ticker"]),
        tickerless_rows_do_not_overlap_company_rows,
    ],
)
def test_quality_invariants_hold(master, check):
    assert check(master.lazy()).collect().is_empty()


def test_valid_from_before_valid_to(master):
    assert master.filter(pl.col("valid_from") >= pl.col("valid_to")).is_empty()


# --- ticker normalization ---


@pytest.mark.parametrize(
    "raw, known, expected",
    [
        ("goog, GOOGL", None, ["GOOG", "GOOGL"]),
        ("BRK.B", {"BRKB": "BRK-B"}, ["BRK-B"]),
        ("ALDF.U", {"ALDFU": "ALDF.U"}, ["ALDF.U"]),
        ("BAC PRL", None, ["BAC-PRL"]),
        ("BAX (NYSE)", None, ["BAX"]),
        ("NASDAQ: AAPL", None, ["AAPL"]),
        ("XYZ - Nasdaq", None, ["XYZ"]),
        ('"""CCO"""', None, ["CCO"]),
        ("(VIXM)", None, ["VIXM"]),
        ("N/A", None, []),
        ("None", None, []),
        ("Common Stock of the Company", None, []),
        (None, None, []),
    ],
)
def test_normalize_tickers(raw, known, expected):
    assert normalize_tickers(raw, known) == expected


# --- no_overlapping_intervals ---


def test_no_overlapping_intervals_flags_overlaps_only():
    lf = pl.LazyFrame(
        {
            "ticker": ["A", "A", "B", "B", "C", None, None],
            "cik": [1, 2, 1, 2, 1, 1, 2],
            "valid_from": [
                date(2020, 1, 1),
                date(2021, 1, 1),
                date(2020, 1, 1),
                date(2020, 6, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
            ],
            "valid_to": [
                date(2021, 1, 1),
                None,
                None,
                date(2021, 1, 1),
                None,
                None,
                None,
            ],
        }
    )

    violations = no_overlapping_intervals("ticker", distinct_by="cik")(lf).collect()

    # A: back to back, fine; B: open-ended row overlaps the later one
    assert violations["ticker"].to_list() == ["B"]
