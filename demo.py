from sklearn import *
from analysis.db.queries import run_custom_query
from analysis.utils import label_returns_dynamic

query = """
select
    timeframe,
    symbol,
    open,
    close,
    volume,
    log_return_1d,
    log_return_1w,
    log_return_1m,
    return_1d,
    return_1w,
    return_1m,
    open_rolling_1w,
    open_rolling_1m,
    open_rolling_1q,
    volatility_1w,
    volatility_1m,
    sharpe_1w,
    sharpe_1m,
from read_parquet('./dataplatform/transformed/stock_daily/**/*.parquet', hive_partitioning=true)
where year >= 2000
and open is not null
and close is not null
"""
df = run_custom_query(query)
# one week future returns
df = label_returns_dynamic(df, thresholds=[0.01, 0.03], steps=5)
df = df.dropna(subset="label")
print(df.shape)
print(df.head())
print(df.columns)
print(df.memory_usage().sum() / 1024**3)

print(df["label"].unique())
