import os
from datetime import datetime, timedelta

from binance import Client
from google.cloud import bigquery


class Config:
    binance_api_key = os.environ.get("BINANCE_API_KEY")
    binance_api_secret = os.environ.get("BINANCE_API_SECRET")
    watch_list_table = os.environ.get("BIGQUERY_WATCH_LIST_TABLE")
    output_table = os.environ.get("BIGQUERY_OUTPUT_TABLE")


bq = bigquery.Client()
binance = Client(api_key=Config.binance_api_key, api_secret=Config.binance_api_secret)


def execute(request, context):
    start_timestamp = datetime.timestamp(
        datetime.now().replace(minute=0, second=0) - timedelta(hours=1)
    )

    watch_list_table = bq.get_table(table=Config.watch_list_table)
    watch_list = bq.list_rows(watch_list_table)
    result_rows = []
    for watch_list_item in watch_list:
        (base_asset, quote_asset, derivative_availale) = watch_list_item.values()
        row = aggregate_data(
            base_asset,
            quote_asset,
            start_timestamp,
            fetch_derivative_data=derivative_availale,
        )

        result_rows.append(row)
        print(row)

    result_table = bq.get_table(table=Config.output_table)
    error = bq.insert_rows(table=result_table, rows=result_rows)
    if error:
        print(error)

    return result_rows


def aggregate_data(
    base_asset, quote_asset, start_timestamp, fetch_derivative_data=True
):
    symbol = base_asset + quote_asset
    klines = binance.get_historical_klines(
        symbol=symbol, interval="1h", start_str=str(start_timestamp), limit=1
    )
    [
        open_time,
        open_price,
        high,
        low,
        close_price,
        _,
        close_time,
        quote_asset_volume,
        number_of_trade,
        _,
        taker_buy_quote_asset_volume,
        _,
    ] = klines[0]
    row = {}
    row["base_asset"] = base_asset
    row["quote_asset"] = quote_asset
    row["open_time"] = open_time / 1000
    row["close_time"] = close_time / 1000
    row["open_price"] = float(open_price)
    row["high"] = float(high)
    row["low"] = float(low)
    row["close_price"] = float(close_price)
    row["number_of_trade"] = number_of_trade

    if quote_asset == "USDT":
        row["volume"] = float(quote_asset_volume)
        row["taker_buy_volume"] = float(taker_buy_quote_asset_volume)
    # convert to USDT volume
    else:
        quote_asset_usdt_price = binance.get_ticker(symbol=quote_asset + "USDT")[
            "lastPrice"
        ]
        row["volume"] = float(quote_asset_volume) * float(quote_asset_usdt_price)
        row["taker_buy_volume"] = float(taker_buy_quote_asset_volume) * float(
            quote_asset_usdt_price
        )

    # only for derivative
    if fetch_derivative_data:
        oi = binance.futures_open_interest_hist(
            symbol=symbol, period="1h", start_str=str(start_timestamp), limit=1
        )
        ratio = binance.futures_top_longshort_position_ratio(
            symbol=symbol, period="1h", limit=1
        )
        row["open_interest"] = float(oi[0]["sumOpenInterestValue"])
        row["longshort_position_ratio"] = float(ratio[0]["longShortRatio"])
    else:
        row["open_interest"] = None
        row["longshort_position_ratio"] = None

    return row
