```
The candle builder pipeline is responsible for receiving messages from the normalizer pipeline and create
candles of it and ingest in DB, while passing the candles information to the next pipeline

For each trade message received:

  1. Parse message → get Trade object (symbol, price, quantity, trade_time)
  2. Calculate bucket_ts = floor(trade_time to nearest minute)
  3. Look up state[symbol]:

     ┌─ NOT FOUND (first trade for this symbol ever)
     │   → CREATE new CandleState(bucket_ts, open=price, ...)
     │   → store in state[symbol]
     │   → DONE (no emit yet)
     │
     ├─ FOUND, bucket_ts == state[symbol].bucket_ts  (same minute)
     │   → UPDATE high/low/close/volume/vwap accumulators
     │   → DONE (no emit yet)
     │
     ├─ FOUND, bucket_ts > state[symbol].bucket_ts   (new minute arrived)
     │   → EMIT state[symbol] candle (Kafka + DB) in parallel
     │   → CREATE new CandleState for new bucket_ts with this trade
     │   → store in state[symbol]
     │
     └─ FOUND, bucket_ts < state[symbol].bucket_ts   (late arrival)
         → if within grace period: apply to old candle (already emitted? correction)
         → if outside grace period: log and drop

  Background task (every 5 seconds):
     For each symbol in state:
         if now > state[symbol].bucket_ts + 60s + grace_period:
             EMIT the stale candle
             remove from state (or reset)

  On graceful shutdown (SIGTERM):
     For each symbol in state:
         EMIT with is_partial=True
         clear state
         
```



# Model Structure 
```
```


# Terminology
Open = the price of the first trade in the minute (chronologically earliest)
Close = the price of the last trade in the minute (chronologically latest)
High = the maximum price (this one IS max)
Low = the minimum price (this one IS min)

Volume = the total number of shares traded during the 1-minute candle window.
         It is the accumulation of `quantity` from every individual trade tick received within that bucket.
         Example: if 6 ticks arrive with quantities [100, 80, 50, 120, 75, 57], volume = 482.
         Note: because we use a sampled websocket feed (Finnhub free tier), this will always be
         lower than the full-tape volume shown on TradingView, which aggregates every exchange venue.
         [The full data can be available from Polygon API or Bloomberg].
         Currently all the tests I have done on the data received by Finnhub's free tier API received.

VWAP (per-candle) = the volume-weighted average price of all trades within the 1-minute candle.

         Formula:
             vwap = Σ(price_i × quantity_i) / Σ(quantity_i)

         Implementation:
             During accumulation we store the raw numerator:
                 candle['vwap'] += price * trade.quantity   (running sum of price × qty)

             At emit time (emit_candle) we finalize it:
                 candle.vwap = candle['vwap'] / candle['volume']

         This is a per-candle metric - it reflects the average execution price within that
         single minute, weighted by trade size. It is NOT the session VWAP indicator shown
         on TradingView, which is cumulative from market open and resets daily.

         Session VWAP (for the indicator pipeline) can be derived from stored candles as:
             session_vwap(t) = SUM(candle.vwap * candle.volume) / SUM(candle.volume)
                               for all candles from session open up to time t



# Understanding on GoRoutines
In File - /Project-Root/consumers/candle_builder/handler.py
```
produce_candle_task = produce_candle(producer, candle)
ingest_db_candle_task = _ingest_into_db(pool, candle)
await asyncio.gather(produce_candle_task, ingest_db_candle_task)
```
Here produce_candle and _ingest_into_db are the two coroutines, and produce_candle_task and ingest_db_candle_task contains the coroutine objects for both the functions.

Now if we use await with a function, then it will trigger that cortoutines individually
await ingest_db_candle_task
await produce_candle_task

the first coroutine runs until it completes, and only then does the second coroutine begin. Therefore, the two operations are executed sequentially.

### Gather() function
So, in order to run them asynchronously we need to trigger them together with the same Event loop
hence, we use the gather function
await asyncio.gather(
    produce_candle(producer, candle),   
    _ingest_into_db(pool, candle),     
)

We use a single "await" with the gather function, gather will collect all the tasks and run them as a part of single event loop, i.e asyncronously.

asyncio.gather() schedules all the supplied coroutine objects to run concurrently on the same event loop. Whenever one coroutine reaches an await (for example, waiting for Kafka, the database, or network I/O), the event loop switches to another coroutine that is ready to run. This allows both operations to make progress without waiting for one to finish completely before starting the other.


It is important to note that asyncio.gather() does not create parallel execution on multiple CPU cores. Both coroutines run on the same thread and the same event loop. They execute concurrently, not in parallel, by cooperatively yielding control whenever they perform asynchronous I/O.