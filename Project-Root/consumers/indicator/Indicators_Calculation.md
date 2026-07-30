# Formulas


# The file is demonstrates the formulas used to calculates the indicators




------------------------------------------------------------------------------------------------------
## EMA - Exponential Moving Average

The Exponential Moving Average (EMA) is a refined moving average (MA) that emphasizes recent data points more heavily, offering a crucial edge in tracking price dynamics. Unlike the Simple Moving Average (SMA), which distributes weight equally across data points, the EMA’s technique gives it a distinct advantage in responding swiftly to price fluctuations.


### Formula to calculate EMA

EMA assigns a smoothing factor (α) to each value. The most recent price gets the full weight, everything before it is captured in the previous EMA recursively.

Formula for EMA = α * (Current Price) + (1 - α) * (Previous EMA)
where α = 2 / (N + 1), and N is the number of periods (e.g., 9, 21, 50, 200).


By period we mean the range right? If I say EMA of 9 days then period is 9?
The period is simply the number of observations used to determine the smoothing factor.

```
| Indicator | Period |
| --------- | -----: |
| EMA-9     |      9 |
| EMA-21    |     21 |
| EMA-50    |     50 |
| EMA-200   |    200 |
```

The smoothing factor becomes:
```
| EMA |       α |
| --- | ------: |
| 9   |    0.20 |
| 21  |  0.0909 |
| 50  |  0.0392 |
| 200 | 0.00995 |
```


A larger period means:

Smaller α
More weight given to historical data
Smoother line
Slower reaction to price changes

A smaller period means:

Larger α
Greater emphasis on recent prices
Faster reaction
More sensitivity to short-term movements



### Important points to consider for calculating EMA
1.) EMA of Tth day depends on the EMA of (T-1)th day
2.) The latest price is given higher weightage, and the older prices are given lower weightage. In other words, the older the period, the smaller α → recent prices dominate less → EMA moves slower.

### Note:
Also, EMA is never perfectly accurate from the moment it's seeded. When you first compute EMA(26) at candle 26, it is literally just an SMA no smoothing has happened at all. The more candles you feed into it after that seed, the more the initial SMA seed gets "forgotten" and the EMA converges toward what it should truly be.
Eg: if wehave lets say 14 candles and we are calculating EMa-12 then it might not be that accurate bcz the seed is the avg of first 12 candles. and then we start calculating the EMa which is very near, hence not much smoothing has happenned.
Its better to take 200 candles, so we keep smoothing the EMa and then for the last 12 candles calculate the EMA

### Think it this way
With 34 candles passed to compute_macd:
  EMA(26) gets only 8 more updates after seeding → still heavily influenced by the SMA seed
  MACD series has only 9 values (barely warmed up)
  Signal EMA(9) seeds on those 9 values, no further recursion → just SMA of 9 bad MACD values

With 200 candles passed:
  EMA(26) gets 174 more updates after seeding → initial SMA seed nearly forgotten
  MACD series has ~175 values (properly warmed up)
  Signal EMA(9) seeds on first 9, then recurses for ~166 more → accurate

This is the reason in the compute_ema function, even though we have to calculate ema12, we pass the whole list of candles "closes_oldest_first", so we get to do enough smoothing




So one thing again to clarify and where my confusion is whenever we have calculate ema x , we pass a certain number of candle closes (closing prices), the first x prices will be seeded as SMA and then we will start calculating from the next set of prices and keep smoothing it , and whatever will be the last value that will be our EMA_x

Pass N closes → seed with SMA of first N → recursively smooth through the rest → last value is your EMA.

------------------------------------------------------------------------------------------------------

## RSI - Relative Strength Indicator

The flow is:

Calculate price changes between consecutive closes.
Split changes into gains and losses.
Seed using the average gain and average loss of the first period changes.
Apply Wilder smoothing for subsequent changes.
Calculate RS and RSI.


Each candle's RSI builds on the previous candle's smoothed avg_gain/avg_loss — not on the original seed. This is exactly why storing avg_gain and avg_loss in the DB is the right optimization.

seed      = SMA(differences 1-14)
avg_gain[1] = smooth(seed, diff[15])       ← candle 16's avg_gain
avg_gain[2] = smooth(avg_gain[1], diff[16]) ← candle 17's avg_gain
RSI[17] = 100 - 100/(1 + avg_gain[2]/avg_loss[2])


---


## RSI - Deep Dive (How it actually works and how we implemented it)

Document from where I understood RSI: https://blog.quantinsti.com/rsi-indicator/

### What is RSI?

RSI (Relative Strength Index) is a number between 0 and 100 that tells you whether a stock has been mostly going up or mostly going down over the last N candles (we use 14 candles). 

- RSI close to 100 means the stock has been gaining strongly — it may be overbought.
- RSI close to 0 means the stock has been losing strongly — it may be oversold.
- RSI around 50 means the gains and losses are roughly balanced.

It does NOT look at the raw price. It looks at the size of up-moves vs down-moves over the last 14 candles.


### The basic idea — what data does RSI use?

RSI does not care about the actual closing prices directly. It cares about the **change** between consecutive closes.

For example:
```
Candle 1 close: 100
Candle 2 close: 103  → change = +3  (gain)
Candle 3 close: 101  → change = -2  (loss of 2)
Candle 4 close: 105  → change = +4  (gain)
```

So first, we compute all the price changes. Then we split each change into:
- gain = the positive change (if price went up)
- loss = the absolute value of the negative change (if price went down)

If on a particular candle the price went up by 3, gain = 3 and loss = 0.
If the price went down by 2, gain = 0 and loss = 2.
A change of 0 counts as gain = 0 and loss = 0.


### What is the "Seed"?

The seed is the starting point for RSI calculation. You cannot calculate RSI for candle 1 — you need a history of at least 14+1 = 15 candles (because you need 14 changes, and to get 14 changes you need 15 closes).

The seed is simply the plain average (SMA) of the first 14 gains and the first 14 losses.

```
seed avg_gain = (gain_1 + gain_2 + ... + gain_14) / 14
seed avg_loss = (loss_1 + loss_2 + ... + loss_14) / 14
```

This seed is only computed once, for the very first time RSI is calculated. After that, you never use plain SMA again you use Wilder smoothing.

**A common confusion point:** You need 14 *changes*, not 14 *closes*. To get 14 changes you need 15 closes (because change = close[i] - close[i-1], so 15 closes give you 14 changes). This is why our code checks `len(candle_closes) >= 16` before calling compute_rsi — 16 closes gives us 15 changes, which means we can seed with 14 changes and apply 1 smoothing step, giving us a meaningful RSI.

In simpler terms if we have 15 values in an array, and we are doing arr[i] - arr[i-1]
then we will be able to get only 15 differences, since the seed needs to have 14 differences, we use 15 candles as input.

### What is Wilder Smoothing?

After the seed, every new candle's avg_gain and avg_loss is calculated using a formula called Wilder smoothing (named after J. Welles Wilder who invented RSI). It is NOT a simple average.

```
new_avg_gain = (previous_avg_gain * 13 + current_gain) / 14
new_avg_loss = (previous_avg_loss * 13 + current_loss) / 14
```

In general for period N:
```
new_avg_gain = (previous_avg_gain * (N-1) + current_gain) / N
```

What this does: it keeps 13/14ths of the old average and adds 1/14th of the new value. This gives more weight to the recent data while not completely forgetting the past. It is effectively the same as EMA but with a different alpha formula (alpha = 1/N instead of 2/(N+1)).

**Key point:** Each new candle only needs the previous candle's avg_gain and avg_loss, plus the latest price change. You do NOT need to go back through all 14+ candles every time. This is an O(1) operation.


### Why do we store avg_gain and avg_loss in the database?

Because of Wilder smoothing. Each new candle's RSI depends on the previous candle's avg_gain and avg_loss — not on raw prices. So to compute RSI for the current candle in O(1), we just need:
1. The previous avg_gain and avg_loss (fetched from the indicators table)
2. The price change of the current candle (current close - previous close)

Without storing avg_gain and avg_loss, we would have to re-read all 200+ historical candles and recompute from scratch every single time a new candle arrives. That is slow and wasteful.

Storing them makes the incremental update fast and cheap.


### Two paths in our implementation: Incremental vs Full Recompute

**Path 1 — Incremental (the fast path, used most of the time):**

When a previous indicator row exists in the database with rsi_avg_gain_14 and rsi_avg_loss_14 already stored, we use Wilder smoothing directly:

```
prev_close = closes_oldest_first[-2]   # second-to-last element
curr_close = candle.close              # latest close (same as closes_oldest_first[-1])

current_gain = (curr_close - prev_close) if curr_close > prev_close else 0
current_loss = (prev_close - curr_close) if curr_close < prev_close else 0

rsi_avg_gain_14 = (historical_indicators.rsi_avg_gain_14 * 13 + current_gain) / 14
rsi_avg_loss_14 = (historical_indicators.rsi_avg_loss_14 * 13 + current_loss) / 14
```

Then RS = rsi_avg_gain_14 / rsi_avg_loss_14, and RSI = 100 - (100 / (1 + RS)).

**Path 2 — Full Recompute (the cold start path, used when there is no prior indicator row):**

When there is no prior indicator in the database (first time the pipeline starts, or for a new symbol), we fetch up to 200 historical candles from the candles table, build the full list of closes, and run the complete compute_rsi function from scratch.

compute_rsi does the following:
1. Loops through all available closes and computes all price changes.
2. Seeds avg_gain and avg_loss using the simple average of the first 14 changes.
3. Applies Wilder smoothing for every change after the 14th, updating avg_gain and avg_loss step by step.
4. Returns the final avg_gain, avg_loss, and RSI.

This gives us the most accurate RSI possible given the data we have, and the resulting avg_gain/avg_loss are stored in the DB so future candles can use Path 1.


### What is RS (Relative Strength)?

RS is just the ratio of average gains to average losses:

```
RS = avg_gain / avg_loss
```

RSI is then derived from RS:

```
RSI = 100 - (100 / (1 + RS))
```

Special cases:
- If avg_loss is 0 (all 14 candles were gains, no losses), RS is infinite and RSI = 100.
- If avg_gain is 0 (all 14 candles were losses, no gains), RS is 0 and RSI = 0.


### The closes_oldest_first list, what is it and why does order matter?

Throughout the code, we work with a list called `closes_oldest_first`. This is a list of closing prices arranged from oldest to newest:

```
closes_oldest_first = [oldest_close, ..., second_to_last_close, current_close]
```

So `closes_oldest_first[-1]` is always the current candle's close, and `closes_oldest_first[-2]` is the previous candle's close.

The reason we care about order: to compute changes correctly, we need close[i] - close[i-1]. If the list were in reverse order, the subtraction would give the wrong sign and all gains/losses would be flipped.

The raw data that comes back from the database is newest-first (ORDER BY open_time DESC), so we reverse it. We also prepend the current candle's close to the front of the DB result before reversing, so the current candle is always at the end.


### Summary of the full RSI flow in our pipeline

```
New candle arrives
        ↓
Fetch last 200 candles from DB (oldest to newest after reversal)
Fetch last 1 indicator row from DB (for the previous candle)
        ↓
Does previous indicator have rsi_avg_gain_14 and rsi_avg_loss_14?
        ↓
   YES → Incremental path                    NO → Full recompute path
   Wilder smoothing on current change        compute_rsi() over all history
   O(1) operation                            O(N) but only happens on cold start
        ↓                                           ↓
   new avg_gain, avg_loss, rsi_14       ←   returns avg_gain, avg_loss, rsi_14
        ↓
Store in indicators table (symbol, resolution, open_time, rsi_14, rsi_avg_gain_14, rsi_avg_loss_14, ...)
Produce to Kafka candles-indicators topic
```


### Common confusion points (Q&A style)

**Q: Why do we need 15 candles to get the first RSI value, not 14?**
A: Because RSI is based on *changes* between closes, not the closes themselves. 14 changes require 15 closing prices (you can't compute a change from a single price). The seed avg_gain and avg_loss are averages of those 14 changes.

**Q: Why do we check `len(candle_closes) >= 16` instead of >= 15?**
A: With 15 closes we would get exactly 14 changes — just enough to compute the seed, but there would be 0 Wilder smoothing steps, making the RSI very rough. With 16 closes we get 15 changes: 14 for the seed and 1 Wilder smoothing step. Our check is slightly conservative, but the difference is just one candle and it results in slightly more reliable RSI from the start.

**Q: Is Wilder smoothing the same as EMA?**
A: They are the same concept — both are exponential smoothing — but they use different alpha values. EMA uses alpha = 2/(N+1). Wilder smoothing uses alpha = 1/N. For N=14, EMA alpha = 2/15 ≈ 0.133, Wilder alpha = 1/14 ≈ 0.071. Wilder's is slower-moving/smoother than standard EMA.

**Q: Why store avg_gain and avg_loss in the DB? Can't we just store RSI and recompute?**
A: No. RSI alone is not enough to do the next incremental update. Wilder smoothing requires the actual avg_gain and avg_loss values, not just the final RSI number. If you only stored RSI, you would have to re-read the full price history every time.

**Q: In the incremental path, where does prev_close come from?**
A: From `closes_oldest_first[-2]`. The list contains the current candle's close at the end (index -1) and the previous candle's close second-to-last (index -2). The previous candle's close comes from the historical candles fetched from the DB.



------------------------------------------------------------------------------------------------------
# MACD (Moving Average Convergence Divergence)

Can refer to this for understanding - https://www.alpharithms.com/python-iterables-072512/#google_vignette

Most of the explanation and understanding I had was from the AI.
Below is the AI-written documentation and recaptulation of all our discussions.

## What is MACD trying to tell you?

MACD is basically trying to answer this question: is the short-term price trend moving away from (diverging) or coming back toward (converging) the long-term trend?

When the short-term average is running ahead of the long-term average, it means price has been rising fast recently — momentum is building. When the two averages start coming back together, momentum is fading. The name literally says this — Moving Average Convergence Divergence.

It doesn't care about the raw price directly. It cares about the relationship between a fast EMA and a slow EMA.


## The three outputs of MACD

MACD gives you three numbers, not one:


### 1. MACD Line

```
MACD Line = EMA(12) - EMA(26)
```

EMA(12) is the short-term (fast) trend. EMA(26) is the long-term (slow) trend. The difference between them is the MACD Line.

- Positive → short-term is above long-term → price has been going up → bullish
- Negative → short-term is below long-term → price has been going down → bearish
- Bigger the number (positive or negative), stronger the momentum


### 2. Signal Line

```
Signal Line = EMA(9) of the MACD Line values (not of the closing prices)
```

This is where it gets a bit different. The Signal Line is not an EMA of prices — it's an EMA of the MACD Line values themselves. So you're smoothing the momentum.

This is used to generate actual trade signals. When the MACD Line crosses the Signal Line, that's a buy or sell signal.


### 3. Histogram

```
Histogram = MACD Line - Signal Line
```

This is just the gap between the MACD Line and Signal Line. When it's growing, momentum is accelerating. When it's shrinking toward zero, momentum is fading — might be a reversal coming.


## What is the "zero line" / "zero crossing"?

I had confusion here — there are actually two places where zero matters:

**Zero line of the MACD Line itself:**
When MACD Line = 0, it means EMA(12) = EMA(26). The short-term and long-term are perfectly aligned. When MACD Line crosses zero from below (goes from negative to positive), it means the fast EMA just crossed above the slow EMA — a bullish signal, but a slow one. Most traders prefer the MACD/Signal crossover instead because it's faster.

**Zero line of the Histogram:**
Histogram = 0 when MACD Line = Signal Line. So histogram crossing zero is the same event as MACD Line crossing Signal Line. When histogram goes from negative to positive, MACD crossed above Signal — bullish.

There is no separate "distance from zero line" calculation. The MACD Line value itself IS that distance. If MACD Line = 2.5, you're 2.5 above zero. That's it.


## Trading signals to watch for

```
MACD Line crosses above Signal Line  →  Bullish (potential buy)
MACD Line crosses below Signal Line  →  Bearish (potential sell)

MACD Line crosses above 0            →  Trend turned bullish (slower, weaker signal)
MACD Line crosses below 0            →  Trend turned bearish (slower, weaker signal)

Histogram growing                    →  Momentum strengthening
Histogram shrinking toward 0         →  Momentum fading, possible reversal
```


## Why EMA(12) and EMA(26) specifically?

Historical defaults from Gerald Appel who invented MACD in the late 1970s. Back then US markets traded 6 days a week. EMA(12) ≈ 2 trading weeks, EMA(26) ≈ 1 trading month. These stuck as the universal standard.


## How does compute_ema work — my understanding

When you call `compute_ema(closes_oldest_first, N)`:
- The first N closing prices are seeded as a plain SMA (just a simple average)
- From candle N+1 onwards, you start applying the EMA formula: `alpha * new_price + (1 - alpha) * previous_ema`
where alpha is 2/(N + 1) where N is the period of EMA,
eg: for EMA_12 the period is 12 (or we call it 12 period EMA)
- You keep doing this for every remaining price
- Whatever the last value is when the loop ends — that is your EMA_N

So the more prices you pass to it beyond N, the more smoothing steps happen, and the more accurate the EMA gets. That's why we pass 200 candles even though technically EMA(26) only needs 26 to produce its first value.


## Why we need EMA(12) and EMA(26) stored in the DB

For the incremental path of MACD, we need EMA(12) and EMA(26) of the previous candle. These were already being computed and stored as part of our regular EMA section (ema_12 and ema_26 fields). So MACD gets those for free — no extra DB query needed.

We also need to store macd_signal (the Signal Line value) so that the next candle can do an incremental update of it.


## Why does Signal Line need 34 candles minimum (not just 26)?

I had confusion here. EMA(26) needs 26 candles to produce its first value. But Signal Line = EMA(9) of the MACD Line values. To seed that EMA(9), you need at least 9 MACD Line values.

The first MACD Line value appears at candle 26. Each candle after that gives you one more. So to get 9 MACD Line values:

```
Candle 26  → MACD value #1
Candle 27  → MACD value #2
...
Candle 34  → MACD value #9
```

So you need 34 candles minimum just to get the first Signal Line value. The formula is: 26 + (9 - 1) = 34. The -1 is because candle 26 already gives you MACD #1 for free, so you only need 8 more candles after that.

But in our pipeline we pass 200 candles, so this minimum is never a concern. We use all 200 to get a properly warmed-up Signal Line.


## Why passing only 34 candles is wrong (my original mistake)

I originally tried passing only the last 34 closes to `compute_macd`. The problem is that EMA(26) seeded on those 34 closes would use the SMA of the last 26 closes as its starting point. But those last 26 closes are not candles 1-26 — they're recent candles with specific recent prices. The EMA hasn't been properly "warmed up" through history. You get the right structure but wrong numbers.

The correct approach is to pass the full 200 closes so EMA(26) gets seeded properly at candle 26 and then updated 174 more times before producing the final MACD value.


Under EMA I have added why this smoothing is important

## The compute_macd function — what it does step by step

This was a key confusion: I was calling compute_ema in a loop for each candle, which reseeds from scratch every time and gives you SMA not EMA. The correct approach is to maintain running state:

```
1. Seed ema_12 = SMA of closes[0:12]
2. Roll ema_12 forward through closes[12:26]  ← 14 more updates, gets ema_12 to candle 26
3. Seed ema_26 = SMA of closes[0:26]
4. Both EMAs are now at the same candle (index 25, the 26th candle)
5. Capture first MACD = ema_12 - ema_26       ← don't skip this step!
6. Walk through closes[26:] updating both EMAs at each step, appending ema_12 - ema_26
7. Return the full macd_series list
```

Step 5 is easy to miss. If you start the loop at index 26 without capturing the first MACD at index 25, you lose one data point. The fix is one line before the loop:
```python
macd_series = [ema_12 - ema_26]   # first MACD value at candle 26
```

Step 6 is critical — you must update both EMAs with the same price in the same iteration. If you call compute_ema from scratch in the loop instead, you're reseeding EMA every time and computing SMA, not EMA.


## The two paths in our implementation


### Incremental path (most candles — fast, O(1))

When `historical_indicators.macd_signal` exists in the DB:

```
alpha = 2 / (9 + 1)  = 0.2
macd_line   = ema_12 - ema_26              ← already computed in the EMA section above
macd_signal = 0.2 * macd_line + 0.8 * historical_indicators.macd_signal
macd_histogram = macd_line - macd_signal
```

No history needed beyond what's already computed. The stored macd_signal carries all the memory of past smoothing.


### Cold start path (first time, full recompute)

When no prior macd_signal in DB:

```
1. closes_oldest_first = reversed(candle_closes)  ← full 200 candles
2. macd_series = compute_macd(closes_oldest_first) ← rolling EMA(12), EMA(26), produces ~175 MACD values
3. macd_signal = compute_ema(macd_series, 9)       ← EMA(9) of those MACD values
4. macd_histogram = macd_line - macd_signal
```

After cold start, the next candle uses the incremental path forever.


## Full pipeline flow

```
New candle arrives
        ↓
EMA section already computed ema_12 and ema_26 for current candle
        ↓
macd_line = ema_12 - ema_26  (or None if not enough history)
        ↓
Does historical_indicators.macd_signal exist?
        ↓
   YES → Incremental                    NO → Cold start
   alpha * macd_line +                  compute_macd(closes_oldest_first)
   (1-alpha) * prev_signal              → macd_series (~175 values)
                                        compute_ema(macd_series, 9)
        ↓                                       ↓
   macd_signal                   ←      macd_signal
        ↓
macd_histogram = macd_line - macd_signal
        ↓
Store in indicators table: macd_line, macd_signal, macd_histogram
```


## What we store in the DB and why

```
macd_line      → stored for downstream consumers (signals, charts)
macd_signal    → MUST store — needed for incremental EMA(9) update next candle
macd_histogram → derived but stored for convenience
ema_12         → MUST store — needed for incremental MACD line update next candle
ema_26         → MUST store — needed for incremental MACD line update next candle
```


------------------------------------------------------------------------------------------------------
# Bollinger Bands


## What are Bollinger Bands?

Bollinger Bands are a volatility indicator invented by John Bollinger in the 1980s. While RSI tells you momentum and MACD tells you trend direction, Bollinger Bands tell you how much the price is moving around its average — basically, is the market calm or wild right now?

Visually they look like an envelope around the price chart — a middle line (the average price) with an upper and lower boundary that breathes in and out depending on how volatile the market is. When price is calm, the bands are tight. When price is moving a lot, the bands widen.


## The three outputs

**Middle Band** = SMA(20) of the last 20 closes. This is just a plain 20-period simple average. Nothing fancy. Same as SMA_20.

**Upper Band** = Middle Band + (2 × standard deviation of the last 20 closes)

**Lower Band** = Middle Band − (2 × standard deviation of the last 20 closes)

**Bandwidth** = (Upper − Lower) / Middle — tells you how wide the bands are relative to the price level. A small bandwidth means the market is very calm. A large bandwidth means it's volatile.


## What is standard deviation here?

Standard deviation measures how spread out the last 20 closes are from their average. If all 20 closes were the exact same price, std_dev = 0 and the bands collapse to a single line. If prices are jumping around a lot, std_dev is large and the bands are wide.

The formula:
```
mean     = sum(last 20 closes) / 20    ← this is bb_middle

variance = sum( (each_close - mean)² ) / 20

std_dev  = sqrt(variance)

bb_upper = mean + 2 * std_dev
bb_lower = mean - 2 * std_dev
```

Why divide by N (not N-1)? There are two types of standard deviation — population (÷N) and sample (÷N-1). Bollinger Bands specifically use population standard deviation (÷N). This is not a mistake. John Bollinger deliberately chose this. We do the same.


## Why 2 standard deviations?

In a normal distribution, 95% of values fall within ±2 standard deviations of the mean. So when price touches the upper or lower band, it's statistically "unusual" — it happens only about 5% of the time under normal market conditions. That's what makes it a meaningful signal.


## What the signals mean

```
Price touches or breaks the UPPER band  →  Overbought, or a strong upside breakout
Price touches or breaks the LOWER band  →  Oversold, or a strong downside breakdown
Bands are very NARROW (squeeze)         →  Volatility is extremely low — a big move
                                            is coming soon (direction unknown)
Bands are very WIDE                     →  High volatility, market is already moving hard
Price bouncing between upper and lower  →  No clear trend, just ranging sideways
```

The squeeze is one of the most useful signals. When bands squeeze tight (bandwidth very small), it means calm has lasted too long. Traders watch for the first candle that breaks out strongly after a squeeze to determine direction.


## Why period 20?

John Bollinger's original default. On daily charts, 20 trading days ≈ 1 calendar month. Same reasoning as EMA(26) for MACD. These defaults became universal standards.


## The key difference from RSI and MACD — no stored state needed

RSI stores avg_gain and avg_loss in the DB because each candle's RSI builds on the previous one (Wilder smoothing is recursive). MACD stores macd_signal because each signal update builds on the previous one.

Bollinger Bands are NOT recursive. Every candle's BB depends only on the last 20 closes in a sliding window. Drop the oldest, add the newest, recalculate from scratch. There is no memory of prior candles beyond the window.

So:
- No incremental path
- No cold start path
- No DB state to store
- Just pick the last 20 closes and compute every time

This makes BB the simplest of all the indicators to implement.


## Implementation

```python
def compute_bollinger_bands(closes_newest_first, period=20):
    if len(closes_newest_first) < period:
        return None, None, None, None

    window = closes_newest_first[:period]       # last 20 closes
    mean = sum(window) / period                 # bb_middle = SMA(20)
    variance = sum((p - mean) ** 2 for p in window) / period   # population variance
    std_dev = variance ** 0.5

    upper = mean + 2 * std_dev
    lower = mean - 2 * std_dev
    bandwidth = (upper - lower) / mean if mean != 0 else None

    return upper, mean, lower, bandwidth
```

In our pipeline we don't have a separate `compute_bollinger_bands` function — the logic is written inline in `compute_indicators` using `closes_oldest_first[-20:]` (same 20 closes, just oldest-to-newest order, which doesn't matter for mean and std_dev since both are order-independent).


## What we store in the DB

```
bb_upper     → stored for downstream (signals, charts)
bb_middle    → stored (= SMA_20, useful reference)
bb_lower     → stored for downstream
bb_bandwidth → stored (useful for squeeze detection)
```

Nothing needs to be stored for the next candle's computation. Each candle computes all four values from scratch using the current 200-candle fetch, no extra DB reads needed.


## Full pipeline flow

```
New candle arrives
        ↓
candle_closes already has last 200 closes (newest first)
        ↓
closes_oldest_first[-20:] = last 20 closes in oldest-to-newest order
        ↓
len >= 20? 
   NO  → bb_upper, bb_middle, bb_lower, bb_bandwidth = None
   YES → compute mean, std_dev, upper, lower, bandwidth
        ↓
Store in indicators table: bb_upper, bb_middle, bb_lower, bb_bandwidth
```