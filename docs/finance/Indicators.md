This page will document various indicators we use, how they are defined, what they are supposed to capture intuitively and how we implement them.

## Arithmetic Return

This indicator answers the question "assuming I buy now the asset, how much money am I going to be left with once I sell it in $n$ time frames?". The way this is phrased implies forward looking which we can't do in general, so the actual way we compute return is "given the asset value right now, how much would I be left with if I entered $n$ time frames ago?". This means that indicator is computed ex post on realized prices; it does not forecast future returns.
Given the current price $P_t$ of an asset and the entry value $P_{t-n}$ (value of the asset $n$ time frames ago), the (ratio) return is defined as:
$$R_{t,n} := \frac{(P_t - P_{t-n})}{P_{t-n}}$$
Intuitively, the value $R_{t,n}$ of the return computed as such will be a percentage (normalized to 1), relative to the starting value $P_{t-n}$, of how much the value has changed; if $R_{t,n}>0$ (or equivalently, $P_t$ is more than 100% of $P_{t-n}$) then the asset has increased its value and if we entered the market we are now in a position to sell at a profit, while if $R_{t,n}<0$ then the asset lost value and if we entered the market we would be selling at a loss now.
This return value is always relative to the starting price $P_{t-n}$ of the asset, which can make it difficult to work with. First of all it is antisymmetric under price reversal: if an assets starts at price 100 and rises to 120 it had return of 0.20, but if it goes back from 120 to 100 in this time frame it had a return of -0.17. Moreover, being relative to some starting value makes it so that this metric is difficult to reaggregate over contiguous time frames (computation of annualized return).

Use cases: portfolio P&L, performance reporting

#### References

[What Are Returns in Investing, and How Are They Measured?](https://www.investopedia.com/terms/r/return.asp)
[Financial Ratio Analysis: Definition, Types, Examples, and How to Use](https://www.investopedia.com/terms/r/ratioanalysis.asp)
[What Is Return on Investment (ROI) and How to Calculate It](https://www.investopedia.com/terms/r/returnoninvestment.asp)

## Log Return

This indicator answers the question "assuming I buy now the asset, how much money am I going to be left with once I sell it in $n$ time frames?", much like the standard [[#Arithmetic Return]]. However this metric computes a different function with complementary properties to the ratio one.
Given the current value $P_t$ and the entry value $P_{t-n}$ (value of the asset $n$ time frames ago), the log return is defined as:
$$R_{ln} := ln(P_t/P_{t-n})$$
This definition makes the indicator more independent of the precise starting values of the formula, simplifying aggregation and making value fluctuation symmetric and centered around 0, which helps a lot in stablizing ML models. For example, if an asset starting at price 100 goes up to 120 it has a log return of 0.18 and if it goes back from 120 to 100 it has a negative log return of -0.18.
More precisely, log returns are scale invariant and rescaling both starting and current price by the same factor keeps the return constant: $ln(P_t/P_{t-n}) = ln(\lambda P_t / \lambda P_{t-n})$. Also log returns are normally distributed to a good approximation and are indeed additive over time, good characteristic for a DL model's input feature.
Moreover, in the context of HFT, by Taylor approximations we have that $ln(1+R_{ln}) \approx R_{ln}$, making log returns effective for continuous price modelling require by HFT.

Use cases: time series analysis, volatility clustering (GARCH?), risk modelling, statistical inference

#### References

[What Are Returns in Investing, and How Are They Measured?](https://www.investopedia.com/terms/r/return.asp)
[Understanding Lognormal vs. Normal Distributions in Financial Analysis](https://www.investopedia.com/articles/investing/102014/lognormal-and-normal-distribution.asp)
[Logarithmic (Log) Returns in Finance](https://365financialanalyst.com/knowledge-hub/corporate-finance/log-return/)

## Volatility

Volatility aims at capturing the relative amplitude of fluctuations in the price of a financial instrument.

The idea is to measure how much the price of the asset tends to go "up and down" within a period of time. An asset displaying constant periodic variation (e.g. growing by 1% every day) would have no volatility, while an asset having big daily increases and decreases would display high volatility.
Mathematically, it is computed as the standard deviation of periodic (typically daily) returns over a given period of time. Using log returns, volatility $\sigma$ would be defined as:

$\sigma_{t, n} = stdev_{t, t-n}(R_{ln})$

Where $t$ identifies the time at the end of the reference period, while $t-n$ identifies the time at the beginning of the reference period, which happens n steps before $t$. The computation would therefore consider the standard deviation in returns taken at each of the n steps (conventionally, each step corresponds to a day) along the time period from $t-n$ to $t$.

Volatility can also be computed using arithmetic (percentage) returns, although the most common definition uses log returns.

## Moving Averages

A moving average is the average value of a variable over the $n$ time steps preceding and including a particular time $t$. In finance, it is typically taken over the closing prices of a financial asset for the $n$ days preceding and including day $t$.

Moving averages of asset prices can be plotted to show a "smoothened" view of price movements. The longer the period (i.e. the number $n$ of days over which the average is taken) the greater the smoothening.
They can also be used as part of investment strategies: for instance, one can compare the 50-day moving average of a stock with the 200-day moving average of the same stock to determine if the stock is facing an upward or downward price moovement.

## Sharpe Ratio

The Sharpe Ratio is a useful metric to evaluate the performance of one financial asset or portfolio (i.e. a combination of assets) with one single value.
Particularily, it combines the two most important metrics of asset performance: returns (the more, the better) and volatility (the lower, the better) by putting them into a ratio. It can be interpreted as the asset's returns "relative" to its volatility.

The reason why it's useful to account for volatility is not just because "uncertainty is bad". Let us consider two portfolios, A and B, with identical returns, but with the former having lower volatility. Using leverage (i.e. borrowing money to add to the money we personally invest, so as to amplify both gains and losses from the investment) we can amplify both the returns and volatility of an investment in A, therefore achieving the same volatility of B but with higher returns. For this reason, A can be considered a better portfolio than B. This is quantified by taking the Sharpe Ratio.

$$Sharpe = \frac{R_p - R_f}{\sigma_p}$$

Where:

$R_p$ is the annualized return of portfolio $p$ (meaning the return that $p$ would give over 1 year by behaving exactly like in the reference period).

$R_p$ is the annualized "risk free" return (meaning the return of a financial asset assumed to have zero risk and zero volatility, such as a treasury bond issued by a stable country).

$\sigma_p$ is the volatility of portfolio $p$ over the reference period.

## RSI

RSI (Relative Strength Index) is a momentum indicator (ie an indicator of how much/fast the price of an asset is changing) and tries to compute the magnitude of price changes to detect overbought/oversold. Given a time window of $n$ steps, let $\delta_P$ the price diference for each steps, $G_n$ be the average gain over those $n$ steps and $L_n$ the average loss, defined as:
$$G_n := AVG(max(0, \delta_P ))$$
$$L_n := AVG(|min(0, \delta_P)|)$$
then the RSI is defined as:
$$RSI := 100 - \frac{100}{1+\frac{G_n}{L_n}}$$
This indicator takes values in $[0,100]$, where values >70 should signal an overbought condition and values <30 should indicate and oversold condition. More conservative threasholds would be 80/20

#### References

[What Is a Momentum Indicator? Definition and Common Indicators](https://www.investopedia.com/investing/momentum-and-relative-strength-index/)
[Relative Strength Index (RSI): What It Is, How It Works, and Formula](https://www.investopedia.com/terms/r/rsi.asp)

## P/E Ratio

The P/E ratio is a useful metric to scan for overvalued or undervalued stocks. At the share/stock level, it compares the market price of the stock to the (per-share) profit of the company. At the company level it compares the company's market capitalization (aggregate market value of all shares outstanding) to the company's profit (net income).

$P/E Ratio = \frac{Mkt Cap}{Net Income} = \frac{Share Price * # Shares Outstanding}{Net Income} = \frac{Share Price}{Earnings per Share}$

The average P/E ratio of companies typically varies among industries, with high-growth industries (e.g. tech) exhibiting very high ratios and low-growth, stable industries (e.g. real estate, banks, manufacturing) exhibiting low ratios. This is due to the fact that the market attributes most of the value of high-growth stocks not to the current earnings but to the future ones.

The P/E ratio of a stock can be compared to the average in its industry as a quick sanity check, to understand whether the stock is over (or under) valued compared to its peers.
