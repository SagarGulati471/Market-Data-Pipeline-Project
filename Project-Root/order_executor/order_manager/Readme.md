


Calculation of Intraday Auto Square Off

We have background task which keeps checking if it is the close of current day's market session.
If yes, then it will auto sqare off the open positions, if not it will sleep till the next day's session
close time

```text
        Start loop
            │
            ▼
    Is market open?
       /          \
     NO            YES
      │             │
      ▼             ▼
Calculate next    Process
market open       market data
      │
      ▼
    sleep
      │
      ▼
   continue
      │
      └──────────────► Start loop again
```