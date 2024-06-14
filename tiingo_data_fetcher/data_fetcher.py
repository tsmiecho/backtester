import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


class TiingoDataFetcher:
    def __init__(self, conn: Any, config: dict[str, Any]) -> None:
        self._conn = conn
        self._config = config

    def fetch(self, tickers: list[str]) -> None:
        try:
            cursor = self._conn.cursor()
            for ticker in tickers:
                self._process_ticker(cursor, ticker)
                self._conn.commit()
        finally:
            cursor.close()
            self._conn.close()

    def _process_ticker(self, cursor: Any, ticker: str) -> None:
        LOGGER.info(f"Fetching data for {ticker}")
        r = requests.get(
            f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate=1960-1-1&endDate=2050-6-11&token={self._config["tiingo"]["token"]}",
            timeout=10,
        )
        response = r.json()
        for entry in response:
            date = datetime.fromisoformat(entry["date"])
            sql = "INSERT INTO daily_price (date, ticker, close_price, div_cash, split_factor) VALUES (%s, %s, %s, %s, %s)"
            val = (
                date,
                ticker,
                Decimal(entry["close"]),
                Decimal(entry["divCash"]),
                Decimal(entry["splitFactor"]),
            )
            cursor.execute(sql, val)
        LOGGER.info(f"Ticker {ticker} successfully processed")
