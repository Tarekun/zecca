import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).parents[2]))
import pytest

from etl.ingestion.sec import (
    SecFullIndex,
    SecXbrlCoverPages,
    SecXbrlFilings,
    parse_master_index,
    parse_notes_dataset,
    parse_xbrl_rss,
)

MASTER_IDX = """Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    March 31, 2015
Comments:              webmaster@sec.gov

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
1326801|Facebook Inc|10-K|2015-01-29|edgar/data/1326801/0001326801-15-000006.txt
1000045|NICHOLAS | FINANCIAL INC|10-Q|2015-02-09|edgar/data/1000045/0001193125-15-038970.txt
"""


def test_parse_master_index():
    df = parse_master_index(MASTER_IDX)

    assert df.height == 2
    assert df.row(0) == (
        1326801,
        "Facebook Inc",
        "10-K",
        date(2015, 1, 29),
        "0001326801-15-000006",
        "edgar/data/1326801/0001326801-15-000006.txt",
    )
    # a "|" inside the company name doesn't shift the other fields
    assert df.row(1)[1:3] == ("NICHOLAS | FINANCIAL INC", "10-Q")


class _OfflineFullIndex(SecFullIndex):
    """Serves a fixed master.idx for every quarter and records downloads."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.downloaded: list[tuple[int, int]] = []

    def load_quarter(self, year, quarter, today):
        self.downloaded.append((year, quarter))
        return parse_master_index(MASTER_IDX).with_columns(
            pl.lit(year, pl.Int32).alias("year"),
            pl.lit(quarter, pl.Int32).alias("quarter"),
            pl.lit(today, pl.Date).alias("ingested_at"),
        )


def test_full_index_rerun_only_downloads_open_quarters(tmp_path):
    source = _OfflineFullIndex(dataplatform_root=str(tmp_path))

    source.ingest(today=date(1994, 2, 15))
    assert source.downloaded == [(1993, q) for q in range(1, 5)] + [(1994, 1)]

    # a month later: the past quarters are final, only 1994Q1 is still open
    source.downloaded.clear()
    source.ingest(today=date(1994, 3, 15))
    assert source.downloaded == [(1994, 1)]

    # next quarter: 1994Q1 was last downloaded while still open, so once more
    source.downloaded.clear()
    source.ingest(today=date(1994, 4, 20))
    assert source.downloaded == [(1994, 1), (1994, 2)]

    source.downloaded.clear()
    source.ingest(today=date(1994, 5, 20))
    assert source.downloaded == [(1994, 2)]

    stored = source.read_from_disk().collect()
    assert stored.select("year", "quarter").unique().height == 6
    # rerunning a quarter upserts instead of duplicating its filings
    assert stored.filter(pl.col("year") == 1994, pl.col("quarter") == 1).height == 2


def _notes_zip(member_suffix=".tsv") -> io.BytesIO:
    sub = (
        "adsh\tcik\tname\tsic\tform\tperiod\tfiled\n"
        "A1\t1652044\tALPHABET INC.\t7370\t8-K\t20261031\t20261030\n"
    )
    header = (
        "adsh\ttag\tversion\tddate\tqtrs\tiprx\tlang\tdcml\tdurp\tdatp\tdimh\tdimn"
        "\tcoreg\tescaped\tsrclen\ttxtlen\tfootnote\tfootlen\tcontext\tvalue\n"
    )

    def fact(tag, dimh, value, coreg=""):
        return (
            f"A1\t{tag}\tdei/2026\t20261031\t0\t0\ten-US\t32767\t0\t0\t{dimh}\t1"
            f"\t{coreg}\t0\t3\t3\t\t0\tc-1\t{value}\n"
        )

    txt = (
        header
        + fact("TradingSymbol", "0x1", "GOOGL")
        + fact("SecurityExchangeName", "0x1", "NASDAQ")
        + fact("Security12bTitle", "0x1", "Class A")
        + fact("EntityRegistrantName", "0x0", "Alphabet Inc.")
        + fact("TradingSymbol", "0x2", "SUBCO", coreg="SomeSubsidiary")
        + "A1\tTextBlock\tmultiline value mentioning\tTradingSymbol\t but broken\n"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(f"sub{member_suffix}", sub)
        z.writestr(f"txt{member_suffix}", txt)
        z.writestr(f"num{member_suffix}", "ignored")
    buffer.seek(0)
    return buffer


def test_parse_notes_dataset_keeps_cover_page_facts_only():
    df = parse_notes_dataset(_notes_zip())

    assert sorted(df["tag"].to_list()) == [
        "Security12bTitle",
        "SecurityExchangeName",
        "TradingSymbol",
        "TradingSymbol",
    ]
    assert df["cik"].unique().to_list() == [1652044]
    assert df["filed"].unique().to_list() == [date(2026, 10, 30)]
    googl = df.filter(pl.col("value") == "GOOGL")
    assert googl["form"][0] == "8-K"
    # co-registrant facts are kept in raw, flagged by `coreg`
    assert df.filter(pl.col("value") == "SUBCO")["coreg"][0] == "SomeSubsidiary"


def test_parse_notes_dataset_older_txt_members():
    assert parse_notes_dataset(_notes_zip(".txt")).height == 5 - 1


class _OfflineCoverPages(SecXbrlCoverPages):
    def __init__(self, datasets, **kwargs):
        super().__init__(**kwargs)
        self.datasets = datasets
        self.downloaded: list[str] = []

    def available_datasets(self):
        return {name: f"https://example/{name}_notes.zip" for name in self.datasets}

    def load_dataset(self, url):
        self.downloaded.append(url.rsplit("/", 1)[1].removesuffix("_notes.zip"))
        return parse_notes_dataset(_notes_zip())


def test_cover_pages_rerun_only_downloads_new_datasets(tmp_path):
    source = _OfflineCoverPages(["2019q1", "2019q2"], dataplatform_root=str(tmp_path))
    source.ingest()
    assert source.downloaded == ["2019q1", "2019q2"]

    source.datasets.append("2026_10")
    source.downloaded.clear()
    source.ingest()
    assert source.downloaded == ["2026_10"]

    stored = source.read_from_disk().collect()
    assert sorted(stored["dataset"].unique().to_list()) == [
        "2019q1",
        "2019q2",
        "2026_10",
    ]


# --- monthly XBRL feeds ---


def _xbrl_feed(namespace: str, *items: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="windows-1252"?><rss version="2.0"><channel>'
        + "".join(items)
        + "</channel></rss>"
    ).encode()


def _xbrl_item(namespace, cik, accession, form, filed, *files):
    xbrl_files = "".join(
        f'<edgar:xbrlFile edgar:sequence="{i}" edgar:file="{name}" edgar:type="{kind}" />'
        for i, (name, kind) in enumerate(files, start=1)
    )
    return (
        f"<item><title>x</title>"
        f'<edgar:xbrlFiling xmlns:edgar="{namespace}">'
        f"<edgar:companyName>Co</edgar:companyName>"
        f"<edgar:formType>{form}</edgar:formType>"
        f"<edgar:filingDate>{filed}</edgar:filingDate>"
        f"<edgar:cikNumber>{cik:010d}</edgar:cikNumber>"
        f"<edgar:accessionNumber>{accession}</edgar:accessionNumber>"
        f"<edgar:period>20131231</edgar:period>"
        f"<edgar:xbrlFiles>{xbrl_files}</edgar:xbrlFiles>"
        f"</edgar:xbrlFiling></item>"
    )


OLD_NS = "http://www.sec.gov/Archives/edgar"
NEW_NS = "https://www.sec.gov/Archives/edgar"


def test_parse_xbrl_rss_reads_instance_and_schema_names():
    feed = _xbrl_feed(
        OLD_NS,
        _xbrl_item(
            OLD_NS,
            859014,
            "0000859014-14-000010",
            "10-Q",
            "02/05/2014",
            ("form10q.htm", "10-Q"),
            ("cpwr-20131231.xml", "EX-101.INS"),
            ("cpwr-20131231.xsd", "EX-101.SCH"),
        ),
    )

    df = parse_xbrl_rss(feed)

    assert df.select(
        "cik", "form_type", "filing_date", "instance_file", "schema_file"
    ).row(0) == (
        859014,
        "10-Q",
        date(2014, 2, 5),
        "cpwr-20131231.xml",
        "cpwr-20131231.xsd",
    )


def test_parse_xbrl_rss_handles_https_namespace_and_inline_xbrl():
    # since 2020 the feeds use https://, and inline XBRL filings list no
    # instance document, only the schema
    feed = _xbrl_feed(
        NEW_NS,
        _xbrl_item(
            NEW_NS,
            320193,
            "0000320193-23-000106",
            "10-K",
            "11/03/2023",
            ("aapl-20230930.htm", "10-K"),
            ("aapl-20230930.xsd", "EX-101.SCH"),
        ),
    )

    row = parse_xbrl_rss(feed).row(0, named=True)

    assert row["instance_file"] is None
    assert row["schema_file"] == "aapl-20230930.xsd"


def test_parse_xbrl_rss_fails_on_unparsed_items():
    feed = _xbrl_feed(
        OLD_NS,
        _xbrl_item(
            "https://example.com/another-namespace",
            1,
            "0000000001-14-000001",
            "10-K",
            "02/05/2014",
        ),
    )

    with pytest.raises(ValueError, match="1 items but 0 parsed"):
        parse_xbrl_rss(feed)


class _OfflineXbrlFilings(SecXbrlFilings):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.downloaded: list[tuple[int, int]] = []

    def load_month(self, year, month, today):
        self.downloaded.append((year, month))
        feed = _xbrl_feed(
            OLD_NS,
            _xbrl_item(
                OLD_NS,
                1,
                f"0000000001-{year % 100:02d}-{month:06d}",
                "10-Q",
                f"{month:02d}/15/{year}",
                ("abc-20131231.xsd", "EX-101.SCH"),
            ),
        )
        return parse_xbrl_rss(feed).with_columns(
            pl.lit(year, pl.Int32).alias("year"),
            pl.lit(month, pl.Int32).alias("month"),
            pl.lit(today, pl.Date).alias("ingested_at"),
        )


def test_xbrl_filings_rerun_only_downloads_open_months(tmp_path):
    source = _OfflineXbrlFilings(dataplatform_root=str(tmp_path))

    source.ingest(today=date(2005, 6, 20))
    assert source.downloaded == [(2005, 4), (2005, 5), (2005, 6)]

    # two weeks later: April and May are final, June was still open
    source.downloaded.clear()
    source.ingest(today=date(2005, 7, 10))
    assert source.downloaded == [(2005, 6), (2005, 7)]

    stored = source.read_from_disk().collect()
    assert stored.select("year", "month").unique().height == 4
    assert stored.filter(pl.col("month") == 6).height == 1
