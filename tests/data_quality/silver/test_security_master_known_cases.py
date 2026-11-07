"""Known cases from issue #122, checked against the materialized
`silver.security_master` (skipped if it hasn't been built yet)."""

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.append(str(Path(__file__).parents[3]))
from etl.transformation.silver.security_master import (
    SecurityMasterSilver,
    listed_at,
    ticker_to_cik,
)

_DATAPLATFORM_ROOT = Path(__file__).parents[3] / "dataplatform"

META, ALPHABET, GOOGLE_INC, TWITTER, LEHMAN = 1326801, 1652044, 1288776, 1418091, 806085


@pytest.fixture(scope="module")
def master() -> pl.DataFrame:
    model = SecurityMasterSilver(dataplatform_root=str(_DATAPLATFORM_ROOT))
    if not (_DATAPLATFORM_ROOT / "silver" / "security_master").exists():
        pytest.skip("silver.security_master not built")
    return model.read_from_disk().collect()


def _rows(master, cik, ticker=None):
    rows = master.filter(pl.col("cik") == cik)
    if ticker is not None:
        rows = rows.filter(pl.col("ticker") == ticker)
    return rows.sort("valid_from")


def test_fb_to_meta(master):
    fb, meta = _rows(master, META, "FB"), _rows(master, META, "META")

    assert fb["valid_to"].max() == meta["valid_from"].min()
    assert date(2022, 6, 1) <= meta["valid_from"].min() <= date(2022, 8, 31)
    assert meta["valid_to"][-1] is None
    assert ticker_to_cik("FB", date(2020, 1, 2), master=master) == META
    assert ticker_to_cik("META", date(2023, 1, 3), master=master) == META


def test_goog_googl_simultaneous_and_passed_from_google_to_alphabet(master):
    tickers = listed_at(date(2020, 1, 2), require_ticker=True, master=master)
    alphabet = tickers.filter(pl.col("cik") == ALPHABET)["ticker"].to_list()

    assert {"GOOG", "GOOGL"} <= set(alphabet)
    assert ticker_to_cik("GOOG", date(2012, 1, 3), master=master) == GOOGLE_INC
    assert ticker_to_cik("GOOG", date(2020, 1, 2), master=master) == ALPHABET


def test_twitter_delisted_in_2022(master):
    twitter = _rows(master, TWITTER)

    assert twitter["delisting_date"][0] == date(2022, 10, 28)
    assert twitter["valid_to"].max() == twitter["delisting_date"][0]
    assert ticker_to_cik("TWTR", date(2021, 1, 4), master=master) == TWITTER
    assert TWITTER not in listed_at(date(2023, 1, 3), master=master)["cik"].to_list()


def test_lehman_bankrupt_in_2008(master):
    lehman = _rows(master, LEHMAN)
    last_report, delisted = lehman["last_filing_date"][0], lehman["delisting_date"][0]

    assert date(2008, 6, 1) <= last_report <= date(2008, 9, 30)
    assert last_report <= delisted <= date(2008, 12, 31)
    assert lehman["valid_to"].max() == delisted
    assert LEHMAN in listed_at(date(2008, 6, 2), master=master)["cik"].to_list()
    assert LEHMAN not in listed_at(date(2009, 1, 2), master=master)["cik"].to_list()


def test_far_more_companies_than_todays_mapping(master):
    current = pl.read_parquet(
        _DATAPLATFORM_ROOT / "silver" / "company_tickers" / "*.parquet"
    )["cik_str"].n_unique()
    assert master["cik"].n_unique() > 3 * current
