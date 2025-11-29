This page will document various indicators we use, how they are defined, what they are supposed to capture intuitively and how we implement them.

## Ratio Return
This indicator answers the question "assuming I buy now the asset, how much money am I going to be left with once I sell it in $t$ time frames?". The way this is phrased implies forward looking which we can't do in general, so the actual way we compute return is "given the asset value right now, how much would I be left with if I entered t time frames ago?".
Given the current value $V$ and the entry value $E$ (value of the asset $t$ time frames ago), the (ratio) return is defined as:
$$R := (V - E) / E$$
The value $R$ of the return computed as such will be a percentage (normalized to 1), relative to the starting value $E$, of how much the value has changed; if $R>0$ (or equivalently, $V$ is more than 100% of $E$) then the asset has increased its value and if we entered the market we are now in a position to sell at a profit, while if $R<0$ then the asset lost value and if we entered the market we would be selling at a loss now.
This return value is always relative to the starting price $E$ of the asset, which can make it difficult to work with. First of all it is antisymmetric: if an assets starts at price 100 and rises to 120 it had return of 0.20, but if it goes back from 120 to 100 in this time frame it had a return of -0.17. Moreover, being relative to some starting value makes it so that this metric is difficult to reaggregate over contiguous time frames (computation of annualized return)


## Log Return
This indicator answers the question "assuming I buy now the asset, how much money am I going to be left with once I sell it in $t$ time frames?", much like the standard Ratio Return. However this metric computes a different function with complementary properties to the ratio one. 
Given the current value $V$ and the entry value $E$ (value of the asset $t$ time frames ago), the log return is defined as:
$$R_{ln} := ln(V/E)$$
This definition makes the indicator more independent of the precise starting values of the formula, simplifying aggregation and making value fluctuation symmetric and centered around 0, which helps a lot in stablizing ML models. For example, if an asset starting at price 100 goes up to 120 it has a log return of 0.18 and if it goes back from 120 to 100 it has a negative log return of -0.18

