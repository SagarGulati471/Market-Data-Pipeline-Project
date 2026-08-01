# About the Indicator Pipeline

indicator pipeline → INSERT into indicators table
                   → produce to candles-indicators topic  ← trigger

signal pipeline    → consume from candles-indicators topic  ← woken up
                   → SELECT last N rows from indicators table  ← context
                   → evaluate strategy
                   → INSERT signal if triggered


Every new candle's indicators arrive
        ↓
Ask each strategy: "do you see anything?"
        ↓
EMA crossover:    "yes, BUY (strength 0.7)"
RSI reversal:     "no signal"
MACD cross:       "yes, BUY (strength 0.6)"
VWAP confluence:  "yes, BUY (strength 0.8)"
        ↓
Aggregate: 3 out of 4 say BUY, weighted score = 0.72
        ↓
0.72 > 0.5 threshold  →  emit final BUY signal



## Terminologies used in the code

__Signal__: the output of a strategy: BUY, SELL, HOLD

__Trade filter__: a condition that qualifies or rejects a signal (e.g., ema_21 > ema_50 is a trend filter — it doesn't generate the signal, it confirms whether to act on it)

__Strategy__: the complete rule: trigger condition + filters combined

So in our code <mark>prev.ema_9 < prev.ema_21 AND curr.ema_9 > curr.ema_21</mark> is the __trigger__.

while only <mark>curr.ema_21 > curr.ema_50</mark> is the __trade filter__. Together they form the EMA crossover strategy.