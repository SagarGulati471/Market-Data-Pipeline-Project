# Signal Generator Pipeline

## What This Pipeline Does

The signal generator sits right after the indicator pipeline in the processing chain. Every time a new set of indicators is computed for a candle, this pipeline wakes up, evaluates four trading strategies, and decides whether the market is showing a buy opportunity, a sell opportunity, or nothing worth acting on.

The full chain looks like this:

```
Raw Trades → Normalizer → Candle Builder → Indicator → Signal Generator → (Order Executor, next)
```

This pipeline consumes from the `indicators` Kafka topic, runs all strategies, computes a weighted score, and writes the result to both the `signals` DB table and the `signals` Kafka topic for downstream consumers.

---

## How a Signal Is Generated

The core idea is simple: instead of relying on one indicator to make a decision, we ask four independent strategies for their opinion, then combine their votes using weights.

```
Every new candle's indicators arrive
        ↓
Ask each strategy: "do you see anything?"
        ↓
EMA crossover:    BUY   (fast EMA just crossed above slow EMA)
RSI reversal:     HOLD  (RSI not at an extreme)
MACD cross:       BUY   (MACD line crossed above signal line)
VWAP confluence:  BUY   (price above VWAP, RSI in healthy range)
        ↓
Weighted score = 0.25 + 0 + 0.25 + 0.25 = 0.75
        ↓
0.75 >= STRONG_BUY threshold → emit STRONG_BUY signal
```

Every single candle always produces a signal row — even if the result is HOLD. This is intentional: it gives us a full historical record of what each strategy was saying at every point in time, which is essential for backtesting and analysis later.

---

## Terminologies

**Signal** — the output of a strategy: `BUY`, `SELL`, or `HOLD`. Each strategy returns one of these three values.

**Trade filter** — a condition that confirms or rejects a trigger. For example, `ema_21 > ema_50` is a trend filter — it doesn't generate the signal on its own, it just checks whether the broader trend agrees before acting on the crossover.

**Strategy** — the complete rule: trigger condition + trade filters combined into one decision function.

**Weighted score** — a number between -1.0 and +1.0. Each strategy that says BUY adds its weight, each SELL subtracts it. HOLD contributes nothing. The final score determines the signal strength.

**Threshold** — the score boundary a strategy must cross to produce a signal. Stored in the DB so each row is self-documenting.

---

## The Four Strategies

### 1. EMA Crossover (`strategy_EMA_crossover.py`)

**What it detects:** The moment a fast moving average crosses through a slow moving average — the transition point where short-term momentum shifts direction.

**Why crossover and not just "fast > slow":** If you just check `ema_9 > ema_21` on the current candle, you fire a BUY signal on every single candle during the entire uptrend (potentially hundreds). The crossover fires exactly once — at the moment of the transition.

**The trigger:**
```
prev.ema_9 < prev.ema_21  AND  curr.ema_9 > curr.ema_21  →  bullish crossover
prev.ema_9 > prev.ema_21  AND  curr.ema_9 < curr.ema_21  →  bearish crossover
```

**The trade filter:** `curr.ema_21 > curr.ema_50` for BUY, `curr.ema_21 < curr.ema_50` for SELL. This prevents taking a fast crossover signal when the medium-term trend is pointing in the opposite direction — those are usually false signals.

**Returns:** `BUY` / `SELL` / `HOLD`

---

### 2. RSI Reversal (`strategy_RSI_reversal.py`)

**What it detects:** RSI (Relative Strength Index) measures momentum on a 0–100 scale. Below 30 means the asset was beaten down heavily (oversold). Above 70 means it was pumped excessively (overbought). The reversal signal fires when RSI *crosses back* out of these extreme zones.

**The trigger:**
```
prev.rsi_14 < 30  AND  curr.rsi_14 > 30  →  recovering from oversold → BUY
prev.rsi_14 > 70  AND  curr.rsi_14 < 70  →  falling from overbought → SELL
```

Again, the crossover is what matters, not the state. RSI sitting at 28 for 10 candles is not 10 separate buy signals.

**Note on reliability:** RSI reversal is the noisiest of the four strategies on 1-minute charts because RSI oscillates frequently at short timeframes. This is why it carries equal weight for now — it will likely get a lower weight once we have backtesting data.

**Returns:** `BUY` / `SELL` / `HOLD`

---

### 3. MACD Crossover (`strategy_MACD_crossover.py`)

**What it detects:** MACD (Moving Average Convergence Divergence) is itself a difference between two EMAs (12-period minus 26-period). It has its own signal line (a 9-period EMA of the MACD). When these two cross, momentum is shifting.

**The trigger:**
```
prev.macd_line < prev.macd_signal  AND  curr.macd_line > curr.macd_signal  →  BUY
prev.macd_line > prev.macd_signal  AND  curr.macd_line < curr.macd_signal  →  SELL
```

**Strong vs weak signals:** We differentiate based on where the crossover happens relative to the zero line:
- Crossover while both values are **above zero** → momentum already bullish, crossover confirms → stronger BUY signal
- Crossover while both values are **below zero** → momentum still bearish overall, but starting to shift → weaker early BUY signal (still returned as BUY, but the aggregation handles the difference via other strategies not confirming)

Both conditions return `BUY` — the signal strength distinction happens at the aggregation level through the weighted score, not inside the strategy itself.

**Returns:** `BUY` / `SELL` / `HOLD`

---

### 4. VWAP Confluence (`strategy_VWAP_confluence.py`)

**What it detects:** VWAP (Volume Weighted Average Price) is the average price of all trades today, weighted by how much volume happened at each price. It resets at market open each day. Institutional traders (funds, banks) use it as their benchmark — they aim to execute close to VWAP.

- **Price above VWAP** = buyers have been more aggressive than sellers all day. Institutions are net buyers. Bullish bias.
- **Price below VWAP** = sellers dominating. Bearish bias.

The "confluence" part means we also require RSI to agree. We don't act on VWAP alone because price can be above VWAP yet already overextended (RSI > 70). The RSI range filter keeps us out of those overextended entries.

**The trigger:**
```
curr.close_price > curr.vwap_session  AND  50 <= curr.rsi_14 <= 65  →  BUY
curr.close_price < curr.vwap_session  AND  35 <= curr.rsi_14 <= 50  →  SELL
```

**Note:** Unlike the other three strategies, VWAP confluence is a **state check**, not a crossover. It doesn't need the previous indicator at all — it asks "right now, are things aligned?" rather than "did something just change?"

**Returns:** `BUY` / `SELL` / `HOLD`

---

## Aggregation and Final Signal

Each strategy has a weight. Currently all four are equal at 0.25 (sums to 1.0):

```python
STRATEGY_CONFIG = [
    ("strategy_EMA_crossover",   EMA_crossover_check,   0.25),
    ("strategy_RSI_reversal",    RSI_reversal_check,    0.25),
    ("strategy_MACD_crossover",  MACD_crossover_check,  0.25),
    ("strategy_VWAP_confluence", VWAP_confluence_check, 0.25),
]
```

The runner loops through all strategies, accumulates the weighted score, and maps it to a final signal type:

```
weighted_score > 0.75   →  STRONG_BUY   (3+ strategies agree bullish)
weighted_score > 0.50   →  BUY          (2+ strategies agree bullish)
-0.50 to 0.50           →  HOLD         (mixed or no signal)
weighted_score < -0.50  →  SELL
weighted_score < -0.75  →  STRONG_SELL
```

Equal weights are the honest starting point when there's no backtesting data. Once the system is running and we have historical signal outcomes, weights should be adjusted to reflect each strategy's actual win rate (a concept called Information Coefficient in quantitative finance).

---

## Key Design Decisions

**Why always write a row even for HOLD?**
Every candle produces a DB row regardless of outcome. This gives a full audit trail of what every strategy was saying at every point — critical for backtesting and debugging why a signal did or didn't fire.

**Why not use `asyncio.gather` for running the strategies in parallel?**
The strategies are pure CPU arithmetic — a few comparisons and subtractions taking microseconds each. `asyncio.gather` only helps with I/O-bound tasks (network calls, DB queries) where you're waiting on something. For CPU arithmetic, it adds coroutine overhead with no benefit. `multiprocessing` would create true parallelism but the process spawn overhead (~10ms) dwarfs the computation time (~0.01ms). Sequential calls are the right choice here.

**Why not use `multiprocessing` for parallel strategy execution?**
Same reason above. The strategies are so fast that the overhead of spinning up a process pool would be 1000× more expensive than just calling them sequentially.

**Why does VWAP confluence not need the previous indicator?**
The other three strategies detect *transitions* (crossovers) — they need to know what changed between the last candle and this one. VWAP confluence asks about the *current state* of the market, not whether something changed. It doesn't matter what VWAP or RSI were one candle ago.

**Why store `close_price` in the indicators table?**
The VWAP confluence strategy needs to compare the current candle's close price against VWAP. The signal generator only receives the `Indicator` object from Kafka — not the original `Candle`. So the close price is carried forward from the candle builder through the indicator pipeline into the `Indicator` model and DB row, making it available to the signal generator without an extra DB lookup.

**Why separate `SignalType` and `StrategySignal` enums?**
Individual strategies only have three possible opinions: `BUY`, `SELL`, or `HOLD`. They don't have enough information to judge whether a signal is "strong" — that requires seeing all strategies together. The strength (`STRONG_BUY` / `STRONG_SELL`) is determined by the aggregation layer, not any individual strategy. Two enums enforce this separation cleanly.
