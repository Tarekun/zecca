import argparse
import sys
from datetime import date, timedelta
import itertools
import polars as pl

sys.path.append("../..")

from analysis.models.common import run_search
from analysis.models.dt_price_classifier import train_decision_tree, DecisionTreeConfig
from etl.transformation.gold import StocksMlReadyGold
from etl.transformation.gold.stocks_ml_ready import append_future_returns

# execution parameters
DATAPLATFORM_ROOT = "../../dataplatform"
LOOKAHEAD_STEPS = 5
THRESHOLDS = [0.01, 0.03]
LABELS = ["more_3_loss", "1_to_3_loss", "stagnant", "1_to_3_gain", "more_3_gain"]
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


def load_data(days_lookahead: int, start_date: date, end_date: date):
    # data loading
    lf = StocksMlReadyGold(
        labellings=[
            append_future_returns(
                lookahead_steps=days_lookahead,
                thresholds=THRESHOLDS,
                custom_labels=LABELS,
            )
        ],
        dataplatform_root=DATAPLATFORM_ROOT,
    ).build()
    df = (
        lf.select("embedding", "timeframe", "price_movement_class", *FEATURE_LIST)
        .filter(pl.col("timeframe") >= start_date, pl.col("timeframe") <= end_date)
        .collect()
    )
    # expand embedding (list of float) into one column per vector component
    embedding_size = int(df["embedding"].list.len().max())  # type: ignore
    EMBEDDING_COLS = [f"embedding_{i}" for i in range(embedding_size)]
    df = df.with_columns(
        pl.col("embedding").list.to_struct(fields=EMBEDDING_COLS)
    ).unnest("embedding")
    print(f"Raw dataset contains {df.height} rows")

    return df


def cleanup_df(df: pl.DataFrame):
    df = df.drop_nulls(subset="price_movement_class")
    # drop rows with infinite/NaN values, as they conflict with DT training
    float_cols = [c for c, dt in df.schema.items() if dt.is_float()]
    df = df.filter(
        pl.all_horizontal([pl.col(c).is_finite().fill_null(False) for c in float_cols])
    )
    print(f"Rows post clean up: {df.height}")

    return df


def build_class_weight(kind: str, labels: list[str]):
    if kind == "None":
        return None
    if kind == "balanced":
        return "balanced"
    if kind == "weight-top":
        boosted = set(labels[-2:])
    elif kind == "weight-bottom":
        boosted = set(labels[:2])
    else:
        raise ValueError(f"unknown class-weight kind: {kind}")

    return {label: 2 if label in boosted else 1 for label in labels}


def parse_args():
    def csv(cast):
        return lambda raw: [cast(v) for v in raw.split(",")]

    def optional_csv(cast):
        return csv(lambda v: None if v == "None" else cast(v))

    def class_weight_csv(raw: str):
        return [build_class_weight(kind, LABELS) for kind in raw.split(",")]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=date(2005, 1, 1)
    )
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument(
        "--criterion", type=csv(str), default=["gini", "entropy", "log_loss"]
    )
    parser.add_argument("--splitter", type=csv(str), default=["best", "random"])
    parser.add_argument("--max-depth", type=optional_csv(int), default=[10, 20, 40])
    parser.add_argument("--min-samples-split", type=csv(int), default=[2, 20, 200])
    parser.add_argument(
        "--max-features", type=optional_csv(str), default=[None, "sqrt", "log2"]
    )
    parser.add_argument("--ccp-alpha", type=csv(float), default=[0.0, 0.01, 0.1])
    parser.add_argument(
        "--class-weight",
        type=class_weight_csv,
        default=[None, build_class_weight("weight-top", LABELS)],
    )

    return parser.parse_args()


def timebased_split(df: pl.DataFrame, test_days: int):
    x, y = df.drop("price_movement_class", "timeframe"), df["price_movement_class"]
    cutoff = df["timeframe"].max() - timedelta(days=test_days)  # type: ignore
    train_mask = df["timeframe"] < cutoff
    xtrain, xtest = x.filter(train_mask), x.filter(~train_mask)
    ytrain, ytest = y.filter(train_mask), y.filter(~train_mask)
    print(f"Input features for the decision tree: {x.columns}")

    return xtrain, xtest, ytrain, ytest


args = parse_args()
df = load_data(LOOKAHEAD_STEPS, args.start_date, args.end_date)
df = cleanup_df(df)
xtrain, xtest, ytrain, ytest = timebased_split(df, 365)

# DT training
param_grid = {
    "criterion": args.criterion,
    "splitter": args.splitter,
    "max_depth": args.max_depth,
    "min_samples_split": args.min_samples_split,
    "max_features": args.max_features,
    "ccp_alpha": args.ccp_alpha,
    "class_weight": args.class_weight,
}
print(f"Param grid built from cli arguments: {param_grid}")

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
    X_test=xtest.to_numpy(),
    y_test=ytest.to_numpy(),
    base_config=DecisionTreeConfig(),
    extra_params={"feature_list": FEATURE_LIST},
)
