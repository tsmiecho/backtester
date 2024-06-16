import datetime as dt
from decimal import Decimal

import numpy as np
import pandas as pd
from pandas import DataFrame


class StrategyFactorsCalculator:
    @staticmethod
    def calculate_simulation_period(df: DataFrame) -> tuple[Decimal, dt.date]:
        start_date = df["date"][0]
        end_date = df["date"][df["date"].size - 1]
        simulation_time_in_years = Decimal(
            str(round((end_date - start_date) / dt.timedelta(365, 5, 49, 12), 2)),
        )
        return simulation_time_in_years, start_date

    @staticmethod
    def calculate_cagr(initial_amount: Decimal, simulation_time_in_years: Decimal, ending_value: Decimal) -> Decimal:
        n = Decimal("1") / simulation_time_in_years
        return Decimal((ending_value / initial_amount) ** n - 1) * 100

    @staticmethod
    def calcualte_standard_diviation(df: DataFrame) -> Decimal:
        df["date"] = pd.to_datetime(df["date"])
        df_last_day = (
            df.groupby(df["date"].dt.to_period("M")).agg({"date": "max", "portfolio_value": "last"}).reset_index(drop=True)
        )
        df_last_day["monthly_return"] = df_last_day["portfolio_value"].pct_change()
        df_last_day = df_last_day.dropna()
        monthly_std = pd.to_numeric(df_last_day["monthly_return"]).std()
        return monthly_std * np.sqrt(12) * 100
