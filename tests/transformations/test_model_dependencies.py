import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parents[3]))

from etl.transformation.gold.sec_indicators import SecIndicatorsGold
from etl.transformation.gold.stocks_daily import StocksDailyGold
from etl.transformation.silver.candles_daily import CandlesDailySilver
from etl.transformation.silver.company_tickers import CompanyTickersSilver
from etl.transformation.silver.sec_company_facts import SecCompanyFactsSilver
from etl.transformation.silver.sec_company_facts_padded import (
    SecCompanyFactsPaddedSilver,
)
from etl.transformation.silver.sec_indicators import SecIndicatorsSilver
from etl.transformation.silver import *
from etl.transformation.silver.stocks_daily import StocksDailySilver

# Each of these models used to declare its dependencies by hand via
# configure_dependencies(); that call was removed once Model.dependencies
# started statically discovering them by walking _build()'s own source
# (see Model._discover_dependencies). Each case pairs a model instance with
# the exact set it used to be manually configured with.
_CASES = [
    (SecCompanyFactsSilver(), {CompanyTickersSilver, CandlesDailySilver}),
    (SecCompanyFactsPaddedSilver(), {SecCompanyFactsSilver}),
    (StocksDailySilver(), {CandlesDailySilver, SecCompanyFactsPaddedSilver}),
    (SecIndicatorsGold(), {SecIndicatorsSilver}),
    (StocksDailyGold(), {StocksDailySilver}),
    (SymbolEmbeddingsSilver(), {CandlesDailySilver}),
]


@pytest.mark.parametrize(
    "model, expected_dependencies",
    _CASES,
    ids=[type(model).__name__ for model, _ in _CASES],
)
def test_discovered_dependencies_match_removed_manual_config(
    model, expected_dependencies
):
    assert set(model.dependencies) == expected_dependencies
