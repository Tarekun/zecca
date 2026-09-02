This page will document the various assets we work with, what they are, how they are traded and why, along with observations and insigths on how that kind of market works

## Stocks

Stocks, or shares, are pieces of ownership in a company. A person or institution holding stocks in a company is called a shareholder of the company. Generally, a stock entitles its holder to 2 things:

1. A share of the company's dividends, i.e. the portion of the profits that the company chooses to distribute to its shareholders

2. Voting rights over major corporate decisions (e.g. the appointment of the company's board)

Not all stock types, however, entitle the holder to both. Below are the most common stock types issued by companies:

a. Common stock: as the name suggests, this is the most common type of stock. It holds both dividend and voting rights. When a company only issues common stocks, each one holds voting and dividend rights equal to 1 over the number of stocks outstanding. Therefore, these 2 rights are proportional to the number of stocks held by the owner. Dividends earned are always dependent on the company's profitability, on the company's decision to distribute dividends or not, and on the percentage of profits that the company will choose to pay out through dividends. Some companies rarely, if ever, distribute dividends even when profitable.

b. Preferred stock: when present, they give the holder the right to receive fixed dividends (e.g. €0.10 per stock), with higher priority with respect to common stock. This means that, if the company distributes €1Mln of dividends this year, and all preferred stocks circulating hold rights to an aggregate €800k of dividends, common stockholders will only have access to the remaining €200k. In case of liquidation of the company (when the company goes bankrupt and is dismantled), preferred stockholders have similar priority in the distribution of remaining assets over common stockholders. These priorities usually come at the cost of not having any voting rights.

c. "Class B" or higher class stocks: when present, they grant superior voting rights (like 10 votes per share, with common stock holding 1). This type of share is frequently used when founders want to maintain control over company decisions while also raising significant capital from the market: the company will sell common stock to the market to raise capital, while class B shares stay within an "inner circle" of people keeping most of the voting power.

For publicly traded companies, stocks are bought and sold on financial markets. The intrinsic theoretical value of a stock corresponds to the (discounted) sum of all expected dividends that the company will pay over its lifetime. Ihe Gordon Model provides a simple mathematical structure for this: considering a dividend $D_0$ paid every year, and a yearly discount rate of $r$ (which is used in the same form of an interest rate to "discount" future earnings the farther they happen in time), the value of the stock would be equal to the perpetuity $\frac{D_0}{1+r} + \frac{D_0}{(1+r)^2} + \frac{D_0}{(1+r)^3} ...$. If we then expect dividends to grow by an annual growth rate $g$ (i.e. by a factor of $1 + g$), then the perpetuity becomes $\frac{D_0*(1+g)}{1+r} + \frac{D_0*(1+g)^2}{(1+r)^2} + \frac{D_0*(1+g)^3}{(1+r)^3} ...$. This can be simplified to:

$P = \frac{D_0*(1+g)}{r - g}$

Where P is the theoretical price for the stock. If we identify as $D_1$ the expected dividend to be paid 1 year from now, which is equal to $D_0*(1 + g)$, the formula can be expressed more simply as:

$P = \frac{D_1}{r - g}$

A more articulated version of the Gordon Model is the DCF (Discounted Cash Flow) model, which relaxes the assumption that dividends grow by a fixed rate in the first few years.

## Bonds

Bonds are the second way through which companies finance their operations. A company issuing a bond takes debt from a creditor, typically in exchange for the payment of a periodic interest rate until the repayment of the sum. Similarily to stocks, some bonds can be publicly traded on financial markets.

From the perspective of the company, stocks constitute its so-called equity and bonds constitute (one form of) debt. Equity and debt are the two sources of ownership over company's assets. Although stocks and bonds appear to be intrinsically different, they have many similarities. One can see bonds as a form of "more preferred" preferred stock: they guarantee a fixed payment, not even subordinated to the issuance of dividends, they give priority over the distribution of assets in case of liquidation, and they do not give the bond-holder (or creditor) any voting rights over corporate decisions. A company financing its operations mainly through debt chooses more stringent payment duties in exchange for a lower dilution of ownership.

Let us imagine company A taking in €1000 from a bank and issuing bonds for the same amount. If these bonds hold a fixed interest rate of 5% and are due in 10 years, the bank will receive:

1. €50 every yer for 10 years from the issuing
2. €1000 on the 10th year

If the bond allows this, the bank can then sell the bonds on financial markets. The price of a bond can increase or decrease on the market (such as for stocks), however, the amount of interest paid and the final sum given back at the end of the period do not change. Key properties of the bond are:

1. Maturity period: the number of years between the date when the bond is issued (and the money is borrowed) and the date when the sum is paid back. The latter date is known as maturity date, or simply maturity. Most bonds have standard maturity periods, such as 3Y, 5Y or 10Y.

2. Principal: the amount borrowed, and to be repaid at the end of the period. It is also known as the par value, or face value, of the bond. Most companies issue bonds with fixed amounts, such as €1000.

3. Coupon: the interest paid periodically. It is typically a fixed amount paid every year, although this can vary depending on the type of bond (some bonds can have variable coupons or payment schedules different from once a year). The ratio between the coupon amount and the principal is called coupon rate.

4. Market price: the price at which the bond is bought and sold on the market. It can differ from the principal amount. Investors might value a bond more than the principal if they have greater confidence in the bond being repaid than the original bondholder. Similarily, they will value it less if they expect it to be riskier. Since the coupon amount is typically fixed (differently from stocks, which have variable dividends), the risk of default is generally the only variable determining market price.

5. Yeld to Maturity (YTM): this is what is typically referred to as the market interest rate of the bond. As the market price of a bond changes, the coupon rate loses some of its relevance as a metric for investors, since it refers to the original price of the bond (the principal), not to the current one. The YTM aims to capture the "updated" interest rate of the bond. It is the hypothetical compounded interest rate at which the market value of the bond equals the sum of its (discounted) future cash flows, including the repayment of the principal. For a bond with market price $P_m$, a principal $P_p$, a maturity period of $n$ years and a yearly coupon of $I$, the YTD solves the equation:

$P_m = \frac{I}{1 + YTD} + \frac{I}{(1 + YTD)^2} ... + \frac{I}{(1 + YTD)^n} + \frac{P_p}{(1 + YTD)^n}$
