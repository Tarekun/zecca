"""Tickers read from XBRL file names and the backfill limits of
`silver.security_master`, on synthetic inputs modelled on real cases."""

import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[2]))
from etl.transformation.silver.security_master import (
    backfill_extends_an_observed_ticker,
    company_name_key,
    compute_security_master,
    listed_at,
    rows_end_by_terminal_filings,
    ticker_to_cik,
)

SNAPSHOT = date(2026, 11, 7)

(
    COMPUWARE,
    SHELL,
    PSEG,
    PARENT,
    SUBSIDIARY,
    WENT_PRIVATE,
    AGENT_A,
    AGENT_B,
    AGENT_C,
    ONE_FILING,
    ONE_FILING_PUBLIC,
    ALICO,
    ALCO_INC,
    DOW_CHEMICAL,
    DOW_INC,
    REVIVED,
    TRAILING_REPORT,
    HOLDING,
    UTILITY,
    DELISTED_THEN_REPORTED,
) = range(10, 30)


def _quarterly(start: date, end: date) -> list[date]:
    days, day = [], start
    while day <= end:
        days.append(day)
        day += timedelta(days=91)
    return days


class _Inputs:
    def __init__(self):
        self.filings, self.cover, self.xbrl, self.current = [], [], [], []

    def reports(self, cik, name, days, form="10-Q", accession=None):
        self.filings += [
            {
                "cik": cik,
                "company_name": name,
                "form_type": form,
                "date_filed": d,
                "accession_number": accession(d) if accession else None,
            }
            for d in days
        ]

    def form(self, cik, name, form, day):
        self.reports(cik, name, [day], form)

    def equity(self, cik, name, years):
        # a 5% holder's annual Schedule 13G about the company's shares
        self.reports(cik, name, [date(y, 2, 14) for y in years], "SC 13G")

    def files(self, cik, days, prefix, form="10-Q", accession=None):
        for d in days:
            self.xbrl.append(
                {
                    "accession_number": (
                        accession(d) if accession else f"{cik}-{d.isoformat()}-{form}"
                    ),
                    "cik": cik,
                    "company_name": "",
                    "form_type": form,
                    "filing_date": d,
                    "period": "",
                    "instance_file": f"{prefix}-{d:%Y%m%d}.xml",
                    "schema_file": f"{prefix}-{d:%Y%m%d}.xsd",
                }
            )

    def declares(self, cik, days, ticker, exchange="NASDAQ", form="10-Q"):
        for d in days:
            base = {
                "adsh": f"{cik}-{d.isoformat()}-{form}",
                "cik": cik,
                "company_name": "",
                "form": form,
                "period": "",
                "filed": d,
                "ddate": "",
                "dimh": "0x0",
                "coreg": "",
                "context": "c",
            }
            self.cover.append({**base, "tag": "TradingSymbol", "value": ticker})
            self.cover.append(
                {**base, "tag": "SecurityExchangeName", "value": exchange}
            )

    def build(self) -> pl.DataFrame:
        cover_schema = {c: pl.String for c in self.cover[0]} | {
            "cik": pl.Int64,
            "filed": pl.Date,
        }
        current = pl.DataFrame(
            self.current,
            schema={
                "cik": pl.Int64,
                "ticker": pl.String,
                "company_name": pl.String,
                "exchange": pl.String,
            },
        )
        filings = pl.DataFrame(
            self.filings,
            schema={
                "cik": pl.Int64,
                "company_name": pl.String,
                "form_type": pl.String,
                "date_filed": pl.Date,
                "accession_number": pl.String,
            },
        )
        return compute_security_master(
            filings.lazy(),
            pl.DataFrame(self.cover, schema=cover_schema).lazy(),
            current,
            SNAPSHOT,
            xbrl_filings=pl.DataFrame(self.xbrl).lazy(),
        )


@pytest.fixture(scope="module")
def master() -> pl.DataFrame:
    i = _Inputs()

    # Compuware: CPWR until it went private in December 2014, never
    # declared on a cover page; only its XBRL file names (2009-2014) say so
    compuware = _quarterly(date(1996, 8, 13), date(2014, 11, 6))
    i.reports(
        COMPUWARE, "COMPUWARE CORPORATION", [d for d in compuware if d.year < 2005]
    )
    i.reports(
        COMPUWARE, "Compuware Corp /MI/", [d for d in compuware if d.year >= 2005]
    )
    i.form(COMPUWARE, "COMPUWARE CORP", "25-NSE", date(2014, 12, 15))
    i.equity(COMPUWARE, "COMPUWARE CORP", range(1997, 2015))
    i.files(COMPUWARE, [d for d in compuware if d.year >= 2009], "cpwr")

    # a shell reused by three businesses; the last took CPWR in 2017
    shell = _quarterly(date(1996, 10, 24), date(2026, 8, 14))
    i.reports(
        SHELL, "CREATIVE VENDING CORP", [d for d in shell if d < date(2006, 6, 7)]
    )
    i.reports(
        SHELL,
        "TETRIDYN SOLUTIONS INC",
        [d for d in shell if date(2006, 6, 7) <= d < date(2017, 5, 12)],
    )
    i.reports(
        SHELL, "Ocean Thermal Energy Corp", [d for d in shell if d >= date(2017, 5, 12)]
    )
    i.files(
        SHELL, [d for d in shell if date(2011, 1, 1) <= d < date(2017, 5, 12)], "tdys"
    )
    i.declares(SHELL, [d for d in shell if d >= date(2017, 8, 14)], "CPWR", "OTC")
    i.current.append(
        {
            "cik": SHELL,
            "ticker": "CPWR",
            "company_name": "Ocean Thermal",
            "exchange": None,
        }
    )

    # PSEG names its files `pseg`, but its ticker is PEG
    pseg = _quarterly(date(2009, 5, 1), date(2026, 8, 1))
    i.reports(PSEG, "PUBLIC SERVICE ENTERPRISE GROUP INC", pseg)
    i.equity(PSEG, "PSEG", range(2009, 2027))
    i.files(PSEG, pseg, "pseg")
    i.declares(PSEG, [d for d in pseg if d.year >= 2019], "PEG", "NYSE")
    i.current.append(
        {"cik": PSEG, "ticker": "PEG", "company_name": "PSEG", "exchange": "NYSE"}
    )

    # a holding company and its subsidiary file under the parent's ticker
    group = _quarterly(date(2009, 5, 1), date(2018, 5, 1))
    i.reports(PARENT, "GREAT PLAINS ENERGY INC", group)
    i.form(PARENT, "GREAT PLAINS ENERGY INC", "25", date(2018, 6, 6))
    i.equity(PARENT, "GREAT PLAINS ENERGY INC", range(2002, 2019))
    i.files(PARENT, group, "gxp")
    i.reports(SUBSIDIARY, "KANSAS CITY POWER & LIGHT CO", group)
    i.files(SUBSIDIARY, group, "gxp")

    # private since 2009, still filing for its debt under its old ticker
    private = _quarterly(date(2001, 5, 1), date(2016, 5, 1))
    i.reports(WENT_PRIVATE, "PUGET ENERGY INC", private)
    i.equity(WENT_PRIVATE, "PUGET ENERGY INC", range(2001, 2009))
    i.files(WENT_PRIVATE, [d for d in private if d.year >= 2009], "psd")

    # a filing agent's template prefix on three unrelated companies
    template = _quarterly(date(2012, 2, 1), date(2014, 12, 1))
    for cik in (AGENT_A, AGENT_B, AGENT_C):
        i.reports(cik, f"COMPANY {cik}", template)
        i.files(cik, template, "fil")

    # a single XBRL report: trusted only with evidence of public shares
    i.reports(ONE_FILING, "BUREAU OF SOMETHING", [date(2011, 7, 29)])
    i.files(ONE_FILING, [date(2011, 7, 29)], "abcd")
    i.reports(ONE_FILING_PUBLIC, "SODASTREAM", [date(2018, 3, 22)], "20-F")
    i.equity(ONE_FILING_PUBLIC, "SODASTREAM", [2017, 2018])
    i.files(ONE_FILING_PUBLIC, [date(2018, 3, 22)], "soda", "20-F")

    # ALCO Inc names its files `alco` while Alico trades as ALCO
    alico = _quarterly(date(2009, 5, 1), date(2026, 8, 1))
    i.reports(ALICO, "ALICO, INC.", alico)
    i.declares(ALICO, [d for d in alico if d >= date(2011, 8, 9)], "ALCO")
    i.current.append(
        {"cik": ALICO, "ticker": "ALCO", "company_name": "Alico", "exchange": "NASDAQ"}
    )
    alco = _quarterly(date(2011, 8, 5), date(2014, 8, 1))
    i.reports(ALCO_INC, "ALCO, INC.", alco)
    i.files(ALCO_INC, alco, "alco")

    # Dow Chemical had DOW until Dow Inc. took it in 2019, then kept
    # naming its files `dow` as a subsidiary
    dow = _quarterly(date(2009, 5, 1), date(2026, 8, 1))
    i.reports(DOW_CHEMICAL, "DOW CHEMICAL CO /DE/", dow)
    i.equity(DOW_CHEMICAL, "DOW CHEMICAL CO", range(2000, 2018))
    i.files(DOW_CHEMICAL, dow, "dow")
    dow_inc = [d for d in dow if d >= date(2019, 4, 2)]
    i.form(DOW_INC, "Dow Inc.", "8-A12B", date(2019, 3, 20))
    i.reports(DOW_INC, "Dow Inc.", dow_inc)
    i.declares(DOW_INC, dow_inc, "DOW", "NYSE")
    i.current.append(
        {"cik": DOW_INC, "ticker": "DOW", "company_name": "Dow", "exchange": "NYSE"}
    )

    # stopped reporting for 9 years, then came back as a new business
    i.reports(REVIVED, "REVIVED CORP", _quarterly(date(1995, 2, 1), date(2000, 12, 1)))
    revived = _quarterly(date(2010, 2, 1), date(2014, 12, 1))
    i.reports(REVIVED, "REVIVED CORP", revived)
    i.files(REVIVED, revived, "revv")

    # a holding company and its utility file one combined report, listed in
    # the XBRL feed under the utility only
    combined = _quarterly(date(2009, 10, 30), date(2012, 5, 1))
    joint = lambda d: f"joint-{d.isoformat()}"  # noqa: E731
    i.reports(HOLDING, "PUBLIC SERVICE ENTERPRISE GROUP INC", combined, accession=joint)
    i.reports(UTILITY, "PUBLIC SERVICE ELECTRIC & GAS CO", combined, accession=joint)
    i.equity(HOLDING, "PSEG", range(2009, 2013))
    i.files(UTILITY, combined, "hldg", accession=joint)

    # delisted in February 2011, its first XBRL report filed in September
    gone = _quarterly(date(2004, 3, 1), date(2010, 12, 1))
    i.reports(DELISTED_THEN_REPORTED, "ALLIS CHALMERS ENERGY INC", gone)
    i.form(
        DELISTED_THEN_REPORTED, "ALLIS CHALMERS ENERGY INC", "25-NSE", date(2011, 2, 24)
    )
    i.equity(DELISTED_THEN_REPORTED, "ALLIS CHALMERS", range(2004, 2012))
    i.reports(DELISTED_THEN_REPORTED, "ALLIS CHALMERS ENERGY INC", [date(2011, 9, 2)])
    i.files(DELISTED_THEN_REPORTED, [date(2011, 9, 2)], "aly")

    # deregistered, then filed its final annual report
    i.form(TRAILING_REPORT, "SMITHKLINE BEECHAM CORP", "15-15D", date(1996, 7, 31))
    i.reports(TRAILING_REPORT, "SMITHKLINE BEECHAM CORP", [date(1997, 3, 31)], "10-K")

    return i.build()


def _rows(master, cik, ticker=None):
    rows = master.filter(pl.col("cik") == cik)
    if ticker is not None:
        rows = rows.filter(pl.col("ticker") == ticker)
    return rows.sort("valid_from")


def test_ticker_read_from_file_names_before_cover_pages(master):
    cpwr = _rows(master, COMPUWARE, "CPWR")

    assert ticker_to_cik("CPWR", date(2010, 6, 1), master=master) == COMPUWARE
    assert ticker_to_cik("CPWR", date(2014, 6, 2), master=master) == COMPUWARE
    assert "xbrl_filename" in cpwr["source"].to_list()
    assert cpwr["valid_to"].max() == date(2014, 12, 15)


def test_backfill_ignores_legal_form_changes_of_the_name(master):
    # COMPUWARE CORPORATION -> Compuware Corp /MI/ is the same company
    assert ticker_to_cik("CPWR", date(1998, 1, 2), master=master) == COMPUWARE


def test_backfill_stops_at_a_change_of_company_name(master):
    shell = _quarterly(date(1996, 10, 24), date(2026, 8, 14))
    first_declaration = min(d for d in shell if d >= date(2017, 8, 14))
    first_file = min(d for d in shell if d >= date(2011, 1, 1))

    # CPWR only from its first declaration: the shell's earlier ticker is
    # the one in its file names
    assert _rows(master, SHELL, "CPWR")["valid_from"].min() == first_declaration
    assert ticker_to_cik("CPWR", date(2016, 1, 4), master=master) is None
    assert ticker_to_cik("TDYS", date(2012, 6, 1), master=master) == SHELL
    # which is backfilled to the first filing as TetriDyn, not further back
    # to Creative Vending
    first_as_tetridyn = min(d for d in shell if d >= date(2006, 6, 7))
    assert _rows(master, SHELL, "TDYS")["valid_from"].min() == first_as_tetridyn
    assert first_file > first_as_tetridyn


def test_backfill_stops_at_a_long_reporting_gap(master):
    revv = _rows(master, REVIVED, "REVV")

    assert revv["valid_from"].min() == date(2010, 2, 1)
    assert ticker_to_cik("REVV", date(1999, 1, 4), master=master) is None


def test_mnemonic_prefix_contradicted_by_cover_pages_is_an_alias(master):
    peg = _rows(master, PSEG, "PEG")

    assert _rows(master, PSEG, "PSEG").is_empty()
    # always seen with PEG declared: its earlier files show PEG
    assert ticker_to_cik("PEG", date(2012, 6, 1), master=master) == PSEG
    assert peg.filter(pl.col("valid_from") < date(2019, 1, 1))["source"].to_list() == [
        "xbrl_filename"
    ]


def test_prefix_shared_with_a_subsidiary_goes_to_the_public_company(master):
    assert ticker_to_cik("GXP", date(2014, 6, 2), master=master) == PARENT
    assert _rows(master, SUBSIDIARY, "GXP").is_empty()


def test_no_file_name_ticker_once_the_shares_are_no_longer_public(master):
    # public until 2008: its 2009-2010 reports still count, the later ones
    # end the ticker instead of extending it
    assert ticker_to_cik("PSD", date(2010, 6, 1), master=master) == WENT_PRIVATE
    assert ticker_to_cik("PSD", date(2012, 6, 1), master=master) is None
    assert WENT_PRIVATE in listed_at(date(2012, 6, 1), master=master)["cik"]


def test_combined_report_prefix_goes_to_the_public_co_registrant(master):
    assert ticker_to_cik("HLDG", date(2011, 6, 1), master=master) == HOLDING
    assert _rows(master, UTILITY, "HLDG").is_empty()


def test_report_filed_after_the_delisting_still_shows_the_ticker(master):
    assert (
        ticker_to_cik("ALY", date(2010, 6, 1), master=master) == DELISTED_THEN_REPORTED
    )
    assert (
        ticker_to_cik("ALY", date(2011, 2, 23), master=master) == DELISTED_THEN_REPORTED
    )
    assert ticker_to_cik("ALY", date(2011, 6, 1), master=master) is None


def test_template_prefix_used_by_many_companies_is_ignored(master):
    assert master.filter(pl.col("ticker") == "FIL").is_empty()


def test_single_filing_needs_evidence_of_public_shares(master):
    assert _rows(master, ONE_FILING, "ABCD").is_empty()
    assert ticker_to_cik("SODA", date(2018, 6, 1), master=master) == ONE_FILING_PUBLIC


def test_prefix_of_another_companys_declared_ticker_is_dropped_whole(master):
    alco_inc = _rows(master, ALCO_INC)

    assert alco_inc["ticker"].is_null().all()
    assert ticker_to_cik("ALCO", date(2010, 6, 1), master=master) == ALICO


def test_former_owners_stale_prefix_is_trimmed_not_dropped(master):
    assert ticker_to_cik("DOW", date(2016, 6, 1), master=master) == DOW_CHEMICAL
    assert ticker_to_cik("DOW", date(2020, 6, 1), master=master) == DOW_INC
    dow = _quarterly(date(2009, 5, 1), date(2026, 8, 1))
    dow_inc_first_declaration = min(d for d in dow if d >= date(2019, 4, 2))
    assert (
        _rows(master, DOW_CHEMICAL, "DOW")["valid_to"].max()
        == dow_inc_first_declaration
    )


def test_company_deregistered_before_its_first_report_is_never_alive(master):
    rows = _rows(master, TRAILING_REPORT)

    assert rows.height == 1
    assert rows["valid_from"][0] == rows["valid_to"][0] == date(1996, 7, 31)
    assert TRAILING_REPORT not in listed_at(date(1997, 6, 2), master=master)["cik"]


@pytest.mark.parametrize(
    "check", [backfill_extends_an_observed_ticker, rows_end_by_terminal_filings]
)
def test_quality_invariants_hold(master, check):
    assert check(master.lazy()).collect().is_empty()


@pytest.mark.parametrize(
    "a, b, same",
    [
        ("COMPUWARE CORPORATION", "Compuware Corp /MI/", True),
        ("DOW CHEMICAL CO /DE/", "Dow Chemical Company", True),
        ("Facebook Inc", "Meta Platforms, Inc.", False),
        ("TETRIDYN SOLUTIONS INC", "Ocean Thermal Energy Corp", False),
        ("AT&T CORP", "AT & T Corp.", True),
    ],
)
def test_company_name_key(a, b, same):
    assert (company_name_key(a) == company_name_key(b)) == same
