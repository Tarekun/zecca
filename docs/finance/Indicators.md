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
This indicator answers the question "assuming I buy now the asset, how much money am I going to be left with once I sell it in $t$ time frames?", much like the standard [[#Arithmetic Return]]. However this metric computes a different function with complementary properties to the ratio one. 
Given the current value $P_t$ and the entry value $P_{t-n}$ (value of the asset $t$ time frames ago), the log return is defined as:
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


## Moving Averages


## Sharp Ratio