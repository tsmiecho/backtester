import logging
from decimal import Decimal
from typing import Any

import pandas as pd
from pandas import DataFrame

from backtester.stategy_factors_calculator import StrategyFactorsCalculator

LOGGER = logging.getLogger(__name__)


class GlobalEquityMomentumStrategy:
    def __init__(
        self,
        config: dict[str, Any],
        conn: Any,
        reinvesting_dividends: bool = True,
    ) -> None:
        self._risk_on_tickers = config["gem"]["risk_on_tickers"]
        self._risk_off_tickers = config["gem"]["risk_off_tickers"]
        self._initial_amount = Decimal(config["initial_amount"])
        self._conn = conn
        self._reinvesting_dividends = reinvesting_dividends
        self._risk_free_rate = config["gem"]["risk_free_rate"]

    def run(self) -> DataFrame:
        df = self._fetch_dataframe()
        simulation_time_in_years, start_date = StrategyFactorsCalculator.calculate_simulation_period(df)

        LOGGER.info(f"Risk on tickers: {self._risk_on_tickers}")
        LOGGER.info(f"Risk off tickers: {self._risk_off_tickers}")
        LOGGER.info(f"Risk free rate: {self._risk_free_rate}")
        LOGGER.info(f"Initial date: {start_date}")
        LOGGER.info(f"Initial portfolio value: {self._initial_amount}")
        LOGGER.info(f"Reinvesting dividends: {self._reinvesting_dividends}")
        LOGGER.info(f"Simulation time: {simulation_time_in_years} years")

        ending_value = self._calculate_ending_value(df)

        cagr = StrategyFactorsCalculator.calculate_cagr(self._initial_amount, simulation_time_in_years, ending_value)
        annual_std = StrategyFactorsCalculator.calcualte_standard_diviation(df)

        LOGGER.debug(f"Initial portfolio value: {self._initial_amount}")
        LOGGER.info(f"Ending portfolio value: {ending_value}")
        LOGGER.info(f"CAGR {round(cagr, 2)} %")
        LOGGER.info(f"Standard deviation {round(annual_std, 2)} %")

        return df[["date", "portfolio_value"]].rename(columns={"portfolio_value": "global_equity_momentum"})

    def _fetch_dataframe(self) -> DataFrame:

        sql = """
            SELECT
                `date`,
        """
        for ticker in self._risk_on_tickers + self._risk_off_tickers:
            sql += f"MAX(CASE WHEN ticker = '{ticker}' THEN close_price END) AS close_price_{ticker},"
            sql += f"MAX(CASE WHEN ticker = '{ticker}' THEN div_cash END) AS div_cash_{ticker},"
            sql += f"MAX(CASE WHEN ticker = '{ticker}' THEN split_factor END) AS split_factor_{ticker},"
        sql = sql[:-1]  # remove last comma
        sql += """
            FROM
                daily_price
            WHERE
        """
        tickers = "ticker IN ("
        for ticker in self._risk_on_tickers + self._risk_off_tickers:
            tickers += f"'{ticker}',"
        tickers = tickers[:-1]  # remove last comma
        tickers += ")"
        sql += tickers
        sql += """
            GROUP BY
                `date`
            HAVING
        """
        for ticker in self._risk_on_tickers + self._risk_off_tickers:
            sql += f"close_price_{ticker} IS NOT NULL AND "
        sql = sql[:-4]  # remove last AND
        sql += """
            ORDER BY
                `date`;
        """
        return pd.read_sql(sql=sql, con=self._conn, parse_dates=True)  # type: ignore [call-overload]

    def _calculate_ending_value(
        self,
        df: DataFrame,
    ) -> Decimal:

        df["portfolio_value"] = self._initial_amount
        df["portfolio_value"] = df["portfolio_value"].astype(float)

        return self._initial_amount
