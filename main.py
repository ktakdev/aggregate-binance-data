import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pandas_ta as ta
from binance import Client
from google.cloud import bigquery


class Config:
    binance_api_key = os.environ.get("BINANCE_API_KEY")
    binance_api_secret = os.environ.get("BINANCE_API_SECRET")
    kline_table = os.environ.get("BIGQUERY_KLINE_TABLE")
    summary_table = os.environ.get("BIGQUERY_SUMMARY_TABLE")


binance = Client(api_key=Config.binance_api_key, api_secret=Config.binance_api_secret)


def fetch_watchlist(quote):
    dict = {}
    res = binance.get_exchange_info()
    symbols = res["symbols"]
    for s in symbols:
        status = s["status"]
        if (
            status != "TRADING"
            or s["baseAsset"].endswith("DOWN")
            or s["baseAsset"].endswith("UP")
        ):
            continue

        base_asset = s["baseAsset"]
        quote_asset = s["quoteAsset"]
        if base_asset not in dict:
            dict[base_asset] = []

        dict[base_asset].append(quote_asset)

    result = []
    for base_asset in dict:
        avaiable_quote_assets = dict[base_asset]
        if "BTC" in avaiable_quote_assets and "USDT" in avaiable_quote_assets:
            result.append((base_asset, quote))

    return result


def aggregate(watchlist, start_timestamp, end_timestamp, date):
    result = []
    for (base_asset, quote_asset) in watchlist:
        symbol = base_asset + quote_asset
        klines = binance.get_historical_klines(
            symbol=symbol,
            interval="1d",
            start_str=str(start_timestamp),
            end_str=str(end_timestamp),
            limit=99,
        )
        print("fetch info: ", symbol)
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'end_timestamp',
                   'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                   'taker_buy_quote_asset_volume', 'ignore']
        df = pd.DataFrame(klines, columns=columns)

        price = df.close.tail(1).values[0]
        count = len(df)
        price_sma_7days = ta.sma(df.close, length=7, fillna=True).tail(
            1).values[0] if count >= 7 else None
        price_sma_25days = ta.sma(df.close, length=25, fillna=True).tail(
            1).values[0] if count >= 25 else None
        price_sma_99days = ta.sma(df.close, length=99, fillna=True).tail(
            1).values[0] if count >= 99 else None

        volume = float(df.quote_asset_volume.tail(1).values[0])
        volume_sma_7days = ta.sma(df.quote_asset_volume, length=7).tail(
            1).values[0] if count >= 7 else None
        volume_sma_25days = ta.sma(df.quote_asset_volume, length=25).tail(
            1).values[0] if count >= 25 else None
        volume_sma_99days = ta.sma(df.quote_asset_volume, length=99).tail(
            1).values[0] if count >= 99 else None

        taker_buy_volume = float(
            df.taker_buy_quote_asset_volume.tail(1).values[0])
        number_of_trades = df.number_of_trades.tail(1).values[0]

        result.append((date, base_asset, quote_asset, price, price_sma_7days, price_sma_25days,
                       price_sma_99days, volume, taker_buy_volume, volume_sma_7days, volume_sma_25days, volume_sma_99days, number_of_trades))

    columns = ['date', 'base_asset', 'quote_asset', 'price', 'price_sma_7days', 'price_sma_25days', 'price_sma_99days',
               'volume', 'taker_buy_volume', 'volume_sma_7days', 'volume_sma_25days', 'volume_sma_99days', 'number_of_trades']
    df = pd.DataFrame(result, columns=columns)

    df = df.astype({
        'date': 'datetime64[s]',
        'base_asset': 'string',
        'quote_asset': 'string',
        'price': 'float64',
        'price_sma_7days': 'float64',
        'price_sma_25days': 'float64',
        'price_sma_99days': 'float64',
        'volume': 'float64',
        'taker_buy_volume': 'float64',
        'volume_sma_7days': 'float64',
        'volume_sma_25days': 'float64',
        'volume_sma_99days': 'float64',
        'number_of_trades': 'int'
    })

    df['is_active'] = ~np.isnan(df.price_sma_99days)
    return df


def execute(request, context):
    dt = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )

    start_timestamp = datetime.timestamp(dt - timedelta(days=99))
    end_timestamp = datetime.timestamp(dt)
    date = datetime.timestamp(dt - timedelta(days=1))

    usdt_watchlist = fetch_watchlist("USDT")
    btc_watchlist = fetch_watchlist("BTC")
    df_usdt = aggregate(usdt_watchlist, start_timestamp, end_timestamp, date)
    df_btc = aggregate(btc_watchlist, start_timestamp, end_timestamp, date)

    df_usdt = df_usdt[df_usdt.is_active]
    usdt_total_count = len(df_usdt)
    usdt_total_volume = df_usdt.volume.sum()
    usdt_total_number_of_trades = df_usdt.number_of_trades.sum()
    usdt_price_gc_count_7_25 = len(
        df_usdt[df_usdt.price_sma_7days > df_usdt.price_sma_25days])
    usdt_price_gc_count_25_99 = len(
        df_usdt[df_usdt.price_sma_25days > df_usdt.price_sma_99days])
    usdt_volume_gc_count_7_25 = len(
        df_usdt[df_usdt.volume_sma_7days > df_usdt.volume_sma_25days])
    usdt_volume_gc_count_25_99 = len(
        df_usdt[df_usdt.volume_sma_25days > df_usdt.volume_sma_99days])
    df_usdt['volume_ratio'] = df_usdt.volume / usdt_total_volume

    df_btc = df_btc[df_btc.is_active]
    df_btc.sort_values(by=['volume'], ascending=False, inplace=True)
    btc_total_count = len(df_btc)
    btc_total_volume = df_btc.volume.sum()
    btc_total_number_of_trades = df_btc.number_of_trades.sum()
    btc_price_gc_count_7_25 = len(
        df_btc[df_btc.price_sma_7days > df_btc.price_sma_25days])
    btc_price_gc_count_25_99 = len(
        df_btc[df_btc.price_sma_25days > df_btc.price_sma_99days])
    btc_volume_gc_count_7_25 = len(
        df_btc[df_btc.volume_sma_7days > df_btc.volume_sma_25days])
    btc_volume_gc_count_25_99 = len(
        df_btc[df_btc.volume_sma_25days > df_btc.volume_sma_99days])
    df_btc['volume_ratio'] = df_btc.volume / btc_total_volume

    summary_columns = ['date', 'usdt_total_count', 'usdt_total_volume', 'usdt_total_number_of_trades', 'usdt_price_gc_count_7_25',
                       'usdt_price_gc_count_25_99', 'usdt_volume_gc_count_7_25', 'usdt_volume_gc_count_25_99', 'btc_total_count', 'btc_total_volume', 'btc_total_number_of_trades', 'btc_price_gc_count_7_25',
                       'btc_price_gc_count_25_99', 'btc_volume_gc_count_7_25', 'btc_volume_gc_count_25_99']
    summary_df = pd.DataFrame([(date, usdt_total_count, usdt_total_volume, usdt_total_number_of_trades, usdt_price_gc_count_7_25, usdt_price_gc_count_25_99,
                              usdt_volume_gc_count_7_25, usdt_volume_gc_count_25_99, btc_total_count, btc_total_volume, btc_total_number_of_trades, btc_price_gc_count_7_25, btc_price_gc_count_25_99, btc_volume_gc_count_7_25, btc_volume_gc_count_25_99)], columns=summary_columns)
    summary_df = summary_df.astype({
        'date': 'datetime64[s]',
        'usdt_total_count': 'int',
        'usdt_total_volume': 'float64',
        'usdt_price_gc_count_7_25': 'int',
        'usdt_price_gc_count_25_99': 'int',
        'usdt_volume_gc_count_7_25': 'int',
        'usdt_volume_gc_count_25_99': 'int',
        'btc_total_count': 'int',
        'btc_total_volume': 'float64',
        'btc_price_gc_count_7_25': 'int',
        'btc_price_gc_count_25_99': 'int',
        'btc_volume_gc_count_7_25': 'int',
        'btc_volume_gc_count_25_99': 'int'
    })

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND

    usdt_job = client.load_table_from_dataframe(
        df_usdt, Config.kline_table, job_config=job_config)

    print("Save USDT klines")
    usdt_job.result()
    print("USDT klines have been saved!")

    btc_job = client.load_table_from_dataframe(
        df_btc, Config.kline_table, job_config=job_config)

    print("Save BTC klines")
    btc_job.result()
    print("BTC klines have been saved!")

    summary_job = client.load_table_from_dataframe(
        summary_df, Config.summary_table, job_config=job_config)

    print("Save summary")
    summary_job.result()
    print("Summary have been saved!")


execute(None, None)
