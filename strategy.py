from datetime import datetime
import matplotlib.pyplot as plt
from cache import load_df
from wallet import Wallet, Simulated


STOCKS = 10
SHORT_WINDOW = 25
LONG_WINDOW = 100
THREASHOLD = 0


df = load_df("msft_1d")
# flatten MultiIndex if necessary
df.columns = [
    "_".join(col).strip() if isinstance(col, tuple) else col for col in df.columns
]
print("dataframe loaded")
df["OpenShort"] = df["Open_MSFT"].rolling(window=SHORT_WINDOW).mean()
df["OpenLong"] = df["Open_MSFT"].rolling(window=LONG_WINDOW).mean()
print("rolling avgs computed")


def decide_on_value(
    wallet: Simulated,
    today: datetime,
    last_investment: datetime,
    last_sale: datetime,
):
    rolling_short = row["OpenShort"]
    rolling_history = row["OpenLong"]
    value = row["Open_MSFT"]
    day_to_investment = (today - last_investment).days
    day_to_sale = (today - last_sale).days

    if (
        rolling_short - rolling_history > THREASHOLD
        and day_to_investment > SHORT_WINDOW
    ):
        wallet.invest_fixed("msft", STOCKS, value)
        return (True, False)

    elif rolling_short - rolling_history < THREASHOLD and day_to_sale > SHORT_WINDOW:
        # if we're selling twice in a row just dump the stocks
        # amount = wallet.owned["msft"] if day_to_sale == SHORT_WINDOW + 1 else STOCKS
        amount = STOCKS
        wallet.sell_fixed("msft", amount, value)
        return (False, True)

    return (False, False)


wallet = Simulated()
balances = []
net_worths = []
prices = []
latest_investment = datetime(1970, 1, 1)
latest_sale = datetime(1970, 1, 1)

for data, row in df.iloc[LONG_WINDOW:].iterrows():
    try:
        today = data.to_pydatetime()  # type:ignore
        invested, sold = decide_on_value(wallet, today, latest_investment, latest_sale)
        if invested:
            latest_investment = today
        elif sold:
            latest_sale = today

        networth = wallet.balance + wallet.owned.get("msft", 0) * row["Open_MSFT"]
        print(
            f"giorno {data} totale {wallet.balance}, network {networth}, stock {row["Open_MSFT"]}"
        )
        balances.append(wallet.balance)
        net_worths.append(networth)
        prices.append(100 * row["Open_MSFT"])
    except ValueError as e:
        print(f"bancarotta al giorno {data}")
        print(e)
        break


plt.figure(figsize=(12, 6))
dates = df.iloc[LONG_WINDOW:].index[: len(balances)]
plt.plot(dates, balances, label="Wallet Balance", color="blue")
plt.plot(dates, net_worths, label="Net Worth", color="orange")
plt.plot(dates, prices, label="Stock price", color="green")
plt.xlabel("Date")
plt.ylabel("USD")
plt.title("Wallet Balance and Net Worth Over Time")
plt.legend()
plt.grid(True)
plt.show()
