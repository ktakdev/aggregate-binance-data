import os
from datetime import datetime

from binance import Client
from google.cloud import bigquery

from binance_data_aggregator import BinanceDataAggregator


class Config:
    binance_api_key = os.environ.get("BINANCE_API_KEY")
    binance_api_secret = os.environ.get("BINANCE_API_SECRET")
    output_table = os.environ.get("BIGQUERY_OUTPUT_TABLE")


bq = bigquery.Client()
binance = Client(api_key=Config.binance_api_key, api_secret=Config.binance_api_secret)
aggregator = BinanceDataAggregator(binance)


def execute(request, context):
    watchlist = aggregator.get_watchlist()
    result_rows = []
    dt = datetime.now()
    for (base_asset, quote_asset) in watchlist:
        try:
            row = aggregator.get_historical_kline(base_asset, quote_asset, dt)
            result_rows.append(row)
            print(row)
        except Exception:
            print(f"error occured, skip {base_asset}/{quote_asset}")

    result_table = bq.get_table(table=Config.output_table)
    error = bq.insert_rows(table=result_table, rows=result_rows)
    if error:
        print(error)

    return result_rows
