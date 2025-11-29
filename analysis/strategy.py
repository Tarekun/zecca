from datetime import datetime
import matplotlib.pyplot as plt
from pandas import DataFrame, Series
from typing import Callable
from cache import load_df
from wallet import Simulated


def moving_avg_strategy(
    diff_threashold: float | int,
    reinvest_wait: int,
    resell_wait: int,
    stocks_to_buy: int,
    stocks_to_sell: int,
) -> Callable[[Series, datetime, datetime], int]:
    """Strategy that sells a fixed amount of stocks if rolling avg value goes
    under historical rolling avg and buys if it goes higher. Expects the DataFrame
    to contain keys `OpenShort` and `OpenLong`"""

    def stocks_to_invest(
        current_state: Series, last_investment: datetime, last_sale: datetime
    ) -> int:
        # dependencies
        rolling_latest = current_state["OpenShort"]
        rolling_history = current_state["OpenLong"]
        reference_date = current_state.name.to_pydatetime()  # type: ignore
        day_to_investment = (reference_date - last_investment).days
        day_to_sale = (reference_date - last_sale).days

        if (
            rolling_latest - rolling_history > diff_threashold
            and day_to_investment > reinvest_wait
        ):
            return stocks_to_buy

        elif (
            rolling_latest - rolling_history < diff_threashold
            and day_to_sale > resell_wait
        ):
            # if we're selling twice in a row just dump the stocks
            # amount = wallet.owned["msft"] if day_to_sale == SHORT_WINDOW + 1 else stocks_to_move
            return -stocks_to_sell

        else:
            return 0

    return stocks_to_invest


def simulate_wallet(
    df: DataFrame, strategy: Callable[[Series, datetime, datetime], int]
):
    wallet = Simulated()
    balances = []
    net_worths = []
    prices = []
    latest_investment = datetime(1970, 1, 1)
    latest_sale = datetime(1970, 1, 1)

    for data, row in df.iloc[LONG_WINDOW:].iterrows():
        stocks = strategy(row, latest_investment, latest_sale)
        stock_value = row["Open_MSFT"]
        if stock_value < 0:
            print(f"negative stock {stock_value}")
        if stocks > 0:
            wallet.invest_fixed("msft", stocks, stock_value)
            latest_investment = data.to_pydatetime()  # type: ignore
        elif stocks < 0:
            wallet.sell_fixed("msft", -stocks, stock_value)
            latest_sale = data.to_pydatetime()  # type: ignore

        networth = wallet.balance + wallet.owned.get("msft", 0) * stock_value
        # print(
        #     f"giorno {data} totale {wallet.balance}, network {networth}, stock {row["Open_MSFT"]}"
        # )
        balances.append(wallet.balance)
        net_worths.append(networth)
        prices.append(100 * row["Open_MSFT"])

    return balances, net_worths, prices


def plot_simulation(dates, balances, net_worths, prices):
    plt.figure(figsize=(12, 6))
    plt.plot(dates, balances, label="Wallet Balance", color="blue")
    plt.plot(dates, net_worths, label="Net Worth", color="orange")
    plt.plot(dates, prices, label="Stock price", color="green")
    plt.xlabel("Date")
    plt.ylabel("moneeee")
    plt.title("Wallet Balance and Net Worth Over Time")
    plt.legend()
    plt.grid(True)
    plt.show()


SHORT_WINDOW = 14
LONG_WINDOW = 80
strategy = moving_avg_strategy(0, 25, 25, 10, 50)

df = load_df("msft_1d")
# flatten MultiIndex if necessary
df.columns = [
    "_".join(col).strip() if isinstance(col, tuple) else col for col in df.columns
]
print("dataframe loaded")
df["OpenShort"] = df["Open_MSFT"].rolling(window=SHORT_WINDOW).mean()
df["OpenLong"] = df["Open_MSFT"].rolling(window=LONG_WINDOW).mean()
print("rolling avgs computed")

starting_value = df.iloc[0]["Open_MSFT"]
final_value = df.iloc[-1]["Open_MSFT"]
amount = int(10000 / starting_value)
print(f"baseline would be {amount * final_value}")

balances, net_worths, prices = simulate_wallet(df, strategy)
dates = df.iloc[LONG_WINDOW:].index[: len(balances)]
plot_simulation(dates, balances, net_worths, prices)
