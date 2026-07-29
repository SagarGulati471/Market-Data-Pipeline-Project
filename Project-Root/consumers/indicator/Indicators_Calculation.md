# Formulas


# The file is demonstrates the formulas used to calculates the indicators




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

