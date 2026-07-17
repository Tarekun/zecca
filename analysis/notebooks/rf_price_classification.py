import sys

sys.path.append("../..")
# imports
from datetime import date, timedelta
import itertools
import json
from matplotlib import pyplot as plt
import numpy as np
import polars as pl
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.preprocessing import LabelEncoder
import time

from analysis.datasets.stocks import load_dataset, append_future_returns
from analysis.db.queries import run_custom_query
from analysis.return_classification.random_forest import (
    RandomForestConfig,
    search_hyperparameters,
)
from etl.transformation.silver import GoodSymbolsSilver
from etl.transformation.gold import StocksDailyGold

DATAPLATFORM_ROOT = "../../dataplatform"


FEATURE_LIST = [
    "log_return_1d",
    "log_return_1w",
    "log_return_1m",
    "log_return_30_steps",
    "return_1d",
    "return_1w",
    "return_1m",
    "return_30_steps",
    "open",
    "open_rolling_1_steps_1d",
    "open_rolling_1w",
    "open_rolling_1m",
    "open_rolling_6m",
    "open_rolling_1y",
    # "volatility_1_steps_1d",
    "volatility_1w",
    "volatility_1m",
    "volatility_1y",
    # "sharpe_1_steps_1d",
    "sharpe_1w",
    "sharpe_1m",
    "rsi",
    "rsi_1d",
    "rsi_1w",
    "shares_outstanding",
    "estimated_float_shares",
    "earnings",
    "evaluation",
    "price_to_earnings",
    "float_adjusted_market_cap",
    "earnings_per_share",
]

# lf = load_dataset(FEATURE_LIST, date(2020, 1, 1), date(2025, 1, 1), dataplatform_root=DATAPLATFORM_ROOT)
lf = (
    StocksDailyGold(dataplatform_root=DATAPLATFORM_ROOT)
    .read_from_disk()
    .filter(
        pl.col("timeframe") >= date(2005, 1, 1), pl.col("timeframe") <= date(2025, 1, 1)
    )
    .select("symbol", "timeframe", *FEATURE_LIST)
    .sort("timeframe")
)
good_symbols = GoodSymbolsSilver(dataplatform_root=DATAPLATFORM_ROOT).read_from_disk()
lf = lf.join(good_symbols, on=["timeframe", "symbol"], how="inner")

lf = append_future_returns(lf, lookahead_steps=5, thresholds=[0.01, 0.03])
lf = lf.drop("future_price")
df = lf.collect()
print(f"Loaded {df.height} rows")


# drop latest rows for which the label is not available, and turn the
# category-cut enum into a plain numeric label -- keep the category strings
# (e.g. "(0.01, 0.03]") around so downstream plots/reports can show the
# actual return range a label corresponds to, indexed by its numeric code
df = df.drop_nulls(subset="price_movement_class")
LABEL_NAMES = df["price_movement_class"].cat.get_categories().to_list()
df = df.with_columns(pl.col("price_movement_class").to_physical().alias("label")).drop(
    "price_movement_class"
)

# encode symbols as unique integers
encoder = LabelEncoder()
df = df.with_columns(
    pl.Series("symbol", encoder.fit_transform(df["symbol"].to_numpy()))
)

# drop rows with infinite/NaN values, as they conflict with DT training
# ("timeframe" is kept around for the train/test split further down)
float_cols = [c for c, dt in df.schema.items() if dt.is_float()]
df = df.filter(
    pl.all_horizontal([pl.col(c).is_finite().fill_null(False) for c in float_cols])
)


print(f"working with columns ({df.shape[1]}): {df.columns}")
print(f"total samples {df.shape[0]}")
print(f"memory usage of the dataset {df.estimated_size('gb')}GB")
print(f"computed return labels: {df['label'].unique().sort().to_list()}")
print(df.head())

x, y = df.drop("label", "timeframe"), df["label"]

# use the last year of data as a test set
cutoff = df["timeframe"].max() - timedelta(days=365)
train_mask = df["timeframe"] < cutoff

xtrain, xtest = x.filter(train_mask), x.filter(~train_mask)
ytrain, ytest = y.filter(train_mask), y.filter(~train_mask)
print(f"trainset size {xtrain.shape[0]} rows, {xtrain.estimated_size('gb')}GB")


# hyperparameter grid to search over -- each combination is trained and
# logged as its own mlflow run by `search_hyperparameters`. kept smaller than
# the decision-tree grid: each combination here trains `n_estimators` trees,
# so the same combinatorial grid would be ~n_estimators times more expensive
param_grid = {
    "n_estimators": [50, 100],
    "max_depth": [20, None],
    "max_features": ["sqrt", "log2"],
    "class_weight": [None, "balanced"],
}
overrides = [
    dict(zip(param_grid.keys(), values))
    for values in itertools.product(*param_grid.values())
]
print(f"searching {len(overrides)} hyperparameter combinations")

# no separate validation split exists, so the held-out test set doubles as
# the validation set used to rank hyperparameter combinations
results = search_hyperparameters(
    overrides,
    xtrain.to_numpy(),
    ytrain.to_numpy(),
    xtest.to_numpy(),
    ytest.to_numpy(),
    extra_params={"feature_list": FEATURE_LIST},
)

best = results[0]
print(f"best overrides: {best['overrides']}")
print(f"val_accuracy={best['result'].val_accuracy} val_f1={best['result'].val_f1}")
