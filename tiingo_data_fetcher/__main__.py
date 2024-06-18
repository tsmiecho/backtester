import logging

import mysql.connector

from configuration import read_config
from tiingo_data_fetcher.data_fetcher import TiingoDataFetcher

LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":
    config = read_config("../config/config.yaml")
    logging.basicConfig(level=config["log_level"])
    conn = mysql.connector.connect(
        user=config["mysql"]["user"],
        password=config["mysql"]["password"],
        host=config["mysql"]["host"],
        port=config["mysql"]["port"],
        database=config["mysql"]["database"],
        charset="utf8",
    )

    TiingoDataFetcher(conn=conn, config=config).fetch(tickers=config["fetcher"]["tickers"])
