import yfinance as yf
from cache import *

microsoft = yf.download("MSFT", period="10y", interval="1d")
if microsoft is not None:
    print(microsoft.head)
    save_df(microsoft, "msft")
    load_df("msft")
    print(microsoft.head)

# dat = yf.Ticker("MSFT")

# # get historical market data
# print(dat.history(period="1mo"), end="\n\n\n")

# # options
# print(dat.option_chain(dat.options[0]).calls, end="\n\n\n")

# # get financials
# print(dat.balance_sheet, end="\n\n\n")
# print(dat.quarterly_income_stmt, end="\n\n\n")

# # # dates
# print(dat.calendar, end="\n\n\n")

# # # general info
# print(dat.info, end="\n\n\n")

# # # analysis
# print(dat.analyst_price_targets, end="\n\n\n")

# # # websocket
# # # dat.live()
