import logging
from decimal import Decimal
from typing import Any

import pandas as pd
from pandas import DataFrame

from backtester.stategy_factors_calculator import StrategyFactorsCalculator

LOGGER = logging.getLogger(__name__)


class SingleAllocationStrategy:
    def __init__(
        self,
        config: dict[str, Any],
        conn: Any,
        reinvesting_dividends: bool = True,
    ) -> None:

        self._ticker = config["single_allocation"]["ticker"]
        self._initial_amount = Decimal(config["initial_amount"])
        self._conn = conn
        self._reinvesting_dividends = reinvesting_dividends

    def run(self) -> DataFrame:
        df = self._fetch_dataframe()
        simulation_time_in_years, start_date = StrategyFactorsCalculator.calculate_simulation_period(df)
        initial_stocks_count, initial_cash = divmod(self._initial_amount, Decimal(df["close_price"][0]))

        LOGGER.info(f"Ticker: {self._ticker}")
        LOGGER.info(f"Initial date: {start_date}")
        LOGGER.info(f"Initial stocks count: {initial_stocks_count}")
        LOGGER.info(f"Initial cash: {round(initial_cash, 2)}")
        LOGGER.info(f"Initial portfolio value: {self._initial_amount}")
        LOGGER.info(f"Reinvesting dividends: {self._reinvesting_dividends}")
        LOGGER.info(f"Simulation time: {simulation_time_in_years} years")

        ending_value = self._calculate_ending_value(df, initial_cash, int(initial_stocks_count))
        cagr = StrategyFactorsCalculator.calculate_cagr(self._initial_amount, simulation_time_in_years, ending_value)
        annual_std = StrategyFactorsCalculator.calcualte_standard_diviation(df)

        LOGGER.debug(f"Initial portfolio value: {self._initial_amount}")
        LOGGER.info(f"Ending portfolio value: {ending_value}")
        LOGGER.info(f"CAGR {round(cagr, 2)} %")
        LOGGER.info(f"Standard deviation {round(annual_std, 2)} %")

        return df[["date", "portfolio_value"]].rename(columns={"portfolio_value": "single_allocation"})

    def _fetch_dataframe(self) -> DataFrame:
        sql = f"SELECT date, close_price, div_cash, split_factor FROM daily_price WHERE ticker = '{self._ticker}'"
        return pd.read_sql(sql=sql, con=self._conn, parse_dates=True)  # type: ignore [call-overload]

    def _calculate_ending_value(
        self,
        df: DataFrame,
        initial_cash: Decimal,
        initial_stocks_count: int,
    ) -> Decimal:
        cash = Decimal(str(initial_cash))
        stocks_count = initial_stocks_count
        for index, row in df.iterrows():
            if row["split_factor"] != 1:
                stocks_count = int(Decimal(row["split_factor"]) * stocks_count)
                LOGGER.debug(f"Splitting stocks count {stocks_count} by {row['split_factor']}")
            if row["div_cash"] > 0:
                dividend_total = str(round(Decimal(str(row["div_cash"])) * stocks_count, 2))
                LOGGER.debug(f"Adding dividend of {row["div_cash"]} per stock, total {dividend_total}")
                cash += Decimal(dividend_total)
                if self._reinvesting_dividends:
                    new_stocks, remaining_cash = divmod(cash, Decimal(row["close_price"]))
                    stocks_count += int(new_stocks)
                    cash = Decimal(str(round(remaining_cash, 2)))
                    LOGGER.debug(f"Stocks count {stocks_count} after reinvesting dividend")
            LOGGER.debug(f"{row['date']} - Portfolio stocks count {stocks_count}, remaining cash {cash}")
            df.loc[index, "portfolio_value"] = round(stocks_count * Decimal(row["close_price"]) + cash, 2)  # type: ignore [call-overload]
        df["portfolio_value"] = pd.to_numeric(df["portfolio_value"])
        last_price = Decimal(str(df["close_price"][df["close_price"].size - 1]))
        return stocks_count * last_price + cash
