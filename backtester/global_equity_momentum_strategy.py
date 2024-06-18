import datetime as dt
import logging
from decimal import Decimal
from typing import Any

import pandas as pd
from dateutil import relativedelta
from pandas import DataFrame, Series

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
        self._risk_free_ticker = config["gem"]["risk_free_ticker"]
        self._momentum_period_in_months = config["gem"]["momentum_period_in_months"]
        self._all_tickers = self._risk_on_tickers + self._risk_off_tickers + [self._risk_free_ticker]
        self._current_ticker: str | None = None
        self._cash: Decimal = self._initial_amount
        self._stocks_count: int = 0

    def run(self) -> DataFrame:
        df = self._fetch_dataframe()
        simulation_time_in_years, start_date = self._calculate_simulation_period(df)

        LOGGER.info(f"Risk on tickers: {self._risk_on_tickers}")
        LOGGER.info(f"Risk off tickers: {self._risk_off_tickers}")
        LOGGER.info(f"Risk free ticker: {self._risk_free_ticker}")
        LOGGER.info(f"Initial date: {start_date}")
        LOGGER.info(f"Initial portfolio value: {self._initial_amount}")
        LOGGER.info(f"Reinvesting dividends: {self._reinvesting_dividends}")
        LOGGER.info(f"Simulation time: {simulation_time_in_years} years")

        ending_value = self._calculate_ending_value(df, start_date)

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
        for ticker in self._all_tickers:
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
        for ticker in self._all_tickers:
            tickers += f"'{ticker}',"
        tickers = tickers[:-1]  # remove last comma
        tickers += ")"
        sql += tickers
        sql += """
            GROUP BY
                `date`
            HAVING
        """
        for ticker in self._all_tickers:
            sql += f"close_price_{ticker} IS NOT NULL AND "
        sql = sql[:-4]  # remove last AND
        sql += """
            ORDER BY
                `date`;
        """
        return pd.read_sql(sql=sql, con=self._conn, parse_dates=True)  # type: ignore [call-overload]

    def _calculate_simulation_period(self, df: DataFrame) -> tuple[Decimal, dt.date]:
        start_date = df["date"][0]
        start_date += relativedelta.relativedelta(months=self._momentum_period_in_months)
        end_date = df["date"][df["date"].size - 1]
        simulation_time_in_years = Decimal(
            str(round((end_date - start_date) / dt.timedelta(365, 5, 49, 12), 2)),
        )
        return simulation_time_in_years, start_date

    def _calculate_ending_value(
        self,
        df: DataFrame,
        start_date: dt.date,
    ) -> Decimal:
        next_momentum_calculation_date = start_date
        for index, row in df.iterrows():
            self._handle_dividends_and_spliting(row)
            if row["date"] >= next_momentum_calculation_date:
                next_momentum_calculation_date += relativedelta.relativedelta(months=1)
                LOGGER.debug(f"Setting momentum calculation date to {next_momentum_calculation_date}")
                self._handle_momentum(df, index, row)
            self._add_portfolio_value_to_dataframe(df, index, row)

        df["portfolio_value"] = pd.to_numeric(df["portfolio_value"])
        return Decimal(str(df["portfolio_value"][df["date"].size - 1]))

    def _handle_dividends_and_spliting(self, row: Series) -> None:
        if self._current_ticker and row[f"split_factor_{self._current_ticker}"] != 1:
            self._stocks_count = int(Decimal(row[f"split_factor_{self._current_ticker}"]) * self._stocks_count)
            LOGGER.debug(f"Splitting stocks count {self._stocks_count} by {row[f'split_factor_{self._current_ticker}']}")
        if self._current_ticker and row[f"div_cash_{self._current_ticker}"] > 0:
            dividend_total = str(round(Decimal(str(row[f"div_cash_{self._current_ticker}"])) * self._stocks_count, 2))
            LOGGER.debug(f"Adding dividend of {row[f"div_cash_{self._current_ticker}"]} per stock, total {dividend_total}")
            self._cash += Decimal(dividend_total)
            if self._reinvesting_dividends:
                new_stocks, remaining_cash = divmod(self._cash, Decimal(row[f"close_price_{self._current_ticker}"]))
                self._stocks_count += int(new_stocks)
                self._cash = Decimal(str(round(remaining_cash, 2)))
                LOGGER.debug(f"Stocks count {self._stocks_count} after reinvesting dividend")

    def _handle_momentum(self, df, index, row: Series):
        momentum_by_ticker, ticker_by_momentum = self._clalculate_momentum(df, index, self._get_past_row(row["date"], df))
        if momentum_by_ticker[self._risk_free_ticker] > max([momentum_by_ticker[ticker] for ticker in self._risk_off_tickers]):
            self._process_higher_risk_free_momentum(row)
        else:
            self._process_lower_risk_free_momentum(momentum_by_ticker, row, ticker_by_momentum)

    def _add_portfolio_value_to_dataframe(self, df, index, row: Series):
        LOGGER.debug(f"{row['date']} - Portfolio stocks count {self._stocks_count}, remaining cash {self._cash}")
        if self._current_ticker:
            df.loc[index, "portfolio_value"] = round(
                self._stocks_count * Decimal(row[f"close_price_{self._current_ticker}"]) + self._cash,
                2,
            )  # type: ignore [call-overload]
        else:
            df.loc[index, "portfolio_value"] = self._cash

    def _process_lower_risk_free_momentum(self, momentum_by_ticker, row: Series, ticker_by_momentum):
        selected_ticker = ticker_by_momentum[max(momentum_by_ticker.values())]
        self._process_asset_switch(selected_ticker, row)

    def _process_higher_risk_free_momentum(self, row: Series):
        selected_ticker = self._risk_on_tickers[0]
        self._process_asset_switch(selected_ticker, row)

    def _process_asset_switch(self, selected_ticker: str, row: Series):
        if self._current_ticker == selected_ticker:
            LOGGER.debug("No change is required")
            return
        if self._current_ticker:
            self._go_to_cash(row)
        self._switch_ticker(row, selected_ticker)

    def _clalculate_momentum(self, df, index, past_row: Series):
        momentum_by_ticker = {}
        ticker_by_momentum = {}
        for ticker in [*self._risk_off_tickers, self._risk_free_ticker]:
            current_close_price = Decimal(str(df.loc[index, f"close_price_{ticker}"]))
            past_close_price = Decimal(str(past_row[f"close_price_{ticker}"].values[0]))
            momentum = current_close_price / past_close_price - 1
            momentum_by_ticker[ticker] = momentum
            ticker_by_momentum[momentum] = ticker

        LOGGER.debug(f"Momentum by ticker: {momentum_by_ticker}")
        LOGGER.debug(f"Ticker by momentum: {ticker_by_momentum}")
        return momentum_by_ticker, ticker_by_momentum

    def _go_to_cash(self, row: Series):
        self._cash += self._stocks_count * Decimal(row[f"close_price_{self._current_ticker}"])
        self._stocks_count = 0
        self._current_ticker = None

    def _switch_ticker(self, row: Series, selected_ticker: str):
        LOGGER.info(f"Switching to ticker {selected_ticker}")
        new_stocks, remaining_cash = divmod(self._cash, Decimal(row[f"close_price_{selected_ticker}"]))
        self._cash = Decimal(str(round(remaining_cash, 2)))
        self._stocks_count = int(new_stocks)
        self._current_ticker = selected_ticker

    def _get_past_row(self, current_date: dt.date, df: DataFrame):
        check_date = current_date - relativedelta.relativedelta(months=self._momentum_period_in_months)
        LOGGER.debug(f"Checking momentum from date {check_date}")
        past_df = DataFrame()
        while past_df.empty:
            past_df = df.loc[df["date"] == check_date]
            check_date += relativedelta.relativedelta(days=1)
        return past_df
