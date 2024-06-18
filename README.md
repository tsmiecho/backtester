# Backtester

## Overview
This project is designed to backtest historical data for various financial instruments including stocks, funds, ETFs, currency crosses, indices, bonds, commodities, certificates, and cryptocurrencies. The goal is to provide a comprehensive toolset for analyzing past performance and developing strategies based on historical data.

## Data sources
- ETFs - https://www.tiingo.com/documentation/end-of-day
- S&P 500 - https://portfoliocharts.com/stock-index-calculator/ (note: LCB == S&P 500)
- Indexes - https://www.msci.com/end-of-day-data-search

## GEM
Suggested configuration:
```
  risk_on_tickers:
    - "BND"
  risk_off_tickers:
    - "SPY"
    - "VEU"
  risk_free_ticker: "BIL"
```
