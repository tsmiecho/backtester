import logging
from decimal import Decimal

import mysql.connector

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
    SingleAllocationStrategy(ticker="SSO", initial_amount=Decimal("10000"), conn=conn).run()
