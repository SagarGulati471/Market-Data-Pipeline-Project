# Risk Manager

The risk manager only answers one thing: Is it safe to place this order right now?

The below are the following aspects which Risk Manager checks and based on that makes the decision:


1. Signal staleness check
Was this signal generated more than N seconds ago? If your system had a lag and a signal is 2 minutes old, the market has moved — the signal is stale and potentially dangerous to act on.

signal.timestamp + 30 seconds < now → REJECT


2. Market hours check
Are we within regular trading hours? Slippage is much higher in pre/post market. For US equities (your Finnhub setup), valid hours are 9:30–16:00 ET.

3. Daily loss limit (the kill switch)
Most important check in any algo system. If you've lost more than $X today, stop all trading for the rest of the day. This is what prevents a buggy signal generator from blowing up your account overnight.

today's_realized_loss > max_daily_loss → REJECT ALL orders for today



4. Duplicate signal guard
The same signal fired twice within 30 seconds (same symbol, same side, same strategy)? Ignore the second one. Your signal generator can fire duplicates — the risk manager is the last line of defense.

5. Conflicting position check
Receiving a BUY for AAPL but already have a SELL order pending for AAPL? These conflict reject the new one until the pending order resolves.

6. Max position per symbol
Already holding 100 shares of AAPL and getting another BUY signal? Reject — you're at your limit. Prevents over-concentration in one stock.

7. Max number of open positions
Already in 5 different symbols? Don't open a 6th. Keeps the portfolio manageable and prevents overexposure.

8. Capital per trade limit
The signal implies buying at $150 × 10 shares = $1500. Do you have $1500 available? Is $1500 above your single-trade limit? Reject if so.

9. Order rate limit
Placed 20 orders in the last 60 seconds? Slow down. Broker APIs have rate limits and will reject or ban you if you exceed them. Track order count in a rolling time window.



-----------------------------------------------------------------------------------------------

# Quantity - how do HFTs and algo traders decide?
Your signal tells you direction (BUY/SELL) but not how much. There are several approaches:

1. Fixed quantity — always buy 10 shares. Simple, but ignores account size and price level. Not used professionally.

2. Fixed capital per trade — always spend $X. quantity = floor(max_capital_per_trade / signal_price). You already have max_capital_per_trade in RiskConfig. This is the simplest professional approach and the right one to implement first.

3. Fixed fractional (% of capital) — risk X% of total capital per trade. quantity = floor((total_capital × 0.02) / price). Very common in retail algo trading. Forces position size to scale with account size.

4. Volatility-adjusted sizing (ATR-based) — size inversely proportional to how volatile the asset is. High volatility = smaller position (less risk). quantity = floor(risk_per_trade / (ATR × price)). This is what sophisticated systematic traders use — your indicator pipeline already computes ATR.

