This markdown file will contain any formulas, or notes on specific market metrics.

**Ask**
Lowest price a seller is willing to accept. Investors buy at ask price

**Bid**
Highest price a buyer is willing to pay. Investors sell at bid price

**MidPrice**
The midpoint between the best bid and the best ask. The best bid
is the highest current buy price, and the best ask is the lowest sell price.
So:
miprice = (bid price + ask price) / 2

**Spread**
Distance between the bid and the ask. 
Spread = Ask - Bid
Measures market tightness/liquidity. 

**spread_bps**
Spread scaled relative to price.
spead_bps = (spread/midprice) * 10000

**Rolling VWAP over 1 minute**
VWAP = volume weighted average price
VWAP = SUM(trade price  * trade size) / SUM(trade size)

Tells you average traded price, weighted by size over a window
Only use trades in last 60 seconds

**Return over 1 minute**
How much price changed within the last minute
return_1m = Pt - Pt-60s / Pt-60s

Short horizon price move

**Realized Volatility over 5 minutes**
A measure of how much price has been moving recently. 
Compute a standard deviation of those returns 
realized vol = std(r1, r2, ..., rn)

**Quote Imbalance**
A simple measure of whether displayed size is heavier on the bid or ask.
Quote Imbalance = (bid size - ask size) / (bid size + ask size)

- Closer to +1: bid side stronger
- Closer to -1: ask side stronger 

**Trade Count Over 1 Minute**
How many trades occured in the last 60 seconds

**Volume over 1 minute**
Total traded size in last 60 seconds
