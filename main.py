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
    dt = datetime.now()
    result_rows = aggregator.aggregate_hourly_data(dt)

    result_table = bq.get_table(table=Config.output_table)
    error = bq.insert_rows(table=result_table, rows=result_rows)
    if error:
        print(error)

    return result_rows
