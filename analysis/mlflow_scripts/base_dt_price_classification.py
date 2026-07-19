import sys
from datetime import date, timedelta
import itertools
import polars as pl

sys.path.append("../..")

from analysis.datasets.stocks import append_future_returns
from analysis.return_classification.common import run_search
from analysis.models.dt_price_classifier import train_decision_tree, DecisionTreeConfig
from etl.transformation.gold import StocksMlReadyGold
from etl.transformation.gold.stocks_ml_ready import append_future_returns

# execution parameters
DATAPLATFORM_ROOT = "../../dataplatform"
START_DATE = date(2005, 1, 1)
END_DATE = date(2025, 1, 1)
LOOKAHEAD_STEPS = 5
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
    "volatility_1w",
    "volatility_1m",
    "volatility_1y",
    "sharpe_1w",
    "sharpe_1m",
    "rsi",
    "rsi_1d",
    "rsi_1w",
    "shares_outstanding",
    "estimated_float_shares",
    # "earnings",
    "evaluation",
    "price_to_earnings",
    "float_adjusted_market_cap",
    "earnings_per_share",
]


# data loading
lf = StocksMlReadyGold(
    labellings=[
        append_future_returns(
            lookahead_steps=LOOKAHEAD_STEPS,
            thresholds=[0.01, 0.03],
            custom_labels=[
                "more_3_loss",
                "1_to_3_loss",
                "stagnant",
                "1_to_3_gain",
                "more_3_gain",
            ],
        )
    ],
    dataplatform_root=DATAPLATFORM_ROOT,
).build()
df = (
    lf.select("embedding", "timeframe", "price_movement_class", *FEATURE_LIST)
    .filter(pl.col("timeframe") >= START_DATE, pl.col("timeframe") <= END_DATE)
    .collect()
)
# expand embedding (list of float) into one column per vector component
embedding_size = int(df["embedding"].list.len().max())  # type: ignore
EMBEDDING_COLS = [f"embedding_{i}" for i in range(embedding_size)]
df = df.with_columns(pl.col("embedding").list.to_struct(fields=EMBEDDING_COLS)).unnest(
    "embedding"
)
print(f"Raw dataset contains {df.height} rows")


# clean up
df = df.drop_nulls(subset="price_movement_class")
# drop rows with infinite/NaN values, as they conflict with DT training
float_cols = [c for c, dt in df.schema.items() if dt.is_float()]
df = df.filter(
    pl.all_horizontal([pl.col(c).is_finite().fill_null(False) for c in float_cols])
)
print(f"Rows post clean up: {df.height}")


# train/test split
x, y = df.drop("price_movement_class", "timeframe"), df["price_movement_class"]
cutoff = df["timeframe"].max() - timedelta(days=365)  # type: ignore
train_mask = df["timeframe"] < cutoff
xtrain, xtest = x.filter(train_mask), x.filter(~train_mask)
ytrain, ytest = y.filter(train_mask), y.filter(~train_mask)
print(f"Input features for the decision tree: {x.columns}")


# DT training
param_grid = {
    "criterion": ["gini", "entropy", "log_loss"],
    "splitter": ["best", "random"],
    "max_depth": [5, 10, 20, None],
    "min_samples_split": [2, 20, 200],
    "max_features": [None, "sqrt", "log2"],
    "ccp_alpha": [0.0, 0.01, 0.1],
    "class_weight": [
        None,
        {
            "more_3_loss": 1,
            "1_to_3_loss": 1,
            "stagnant": 1,
            "1_to_3_gain": 1,
            "more_3_gain": 5,
        },
    ],
}
overrides = [
    dict(zip(param_grid.keys(), values))
    for values in itertools.product(*param_grid.values())
]
print(f"searching {len(overrides)} hyperparameter combinations")
results = run_search(
    train_fn=train_decision_tree,
    overrides=overrides,
    X_train=xtrain.to_numpy(),
    y_train=ytrain.to_numpy(),
    X_val=xtest.to_numpy(),
    y_val=ytest.to_numpy(),
    base_config=DecisionTreeConfig(),
    extra_params={"feature_list": FEATURE_LIST},
)
