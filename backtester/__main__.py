import logging

import matplotlib.pyplot as plt
import mysql.connector

from backtester.global_equity_momentum_strategy import GlobalEquityMomentumStrategy
from backtester.single_allocation_strategy import SingleAllocationStrategy
from configuration import read_config

LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = read_config("../config/config.yaml")
    conn = mysql.connector.connect(
        user=config["mysql"]["user"],
        password=config["mysql"]["password"],
        host=config["mysql"]["host"],
        port=config["mysql"]["port"],
        database=config["mysql"]["database"],
        charset="utf8",
    )

    if config["selected_strategy"] == "single_allocation":
        df = SingleAllocationStrategy(
            config=config,
            conn=conn,
        ).run()
    elif config["selected_strategy"] == "gem":
        df = GlobalEquityMomentumStrategy(
            config=config,
            conn=conn,
        ).run()

    df.plot(x="date", figsize=(16, 8))
    plt.tight_layout()
    plt.show()
