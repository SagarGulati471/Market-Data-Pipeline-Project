# Formulas


# The file is demonstrates the formulas used to calculates the indicators




## EMA Exponential Moving Average

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




