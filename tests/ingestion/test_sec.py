import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import polars as pl

sys.path.append(str(Path(__file__).parents[2]))
from etl.ingestion.sec import (
    SecFullIndex,
    SecXbrlCoverPages,
    parse_master_index,
    parse_notes_dataset,
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
