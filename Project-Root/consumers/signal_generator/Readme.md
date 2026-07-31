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