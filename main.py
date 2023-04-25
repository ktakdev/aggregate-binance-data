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


def fetch_watchlist():
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

    result = [("BTC", "USDT")]
    for base_asset in dict:
        avaiable_quote_assets = dict[base_asset]
        if "BTC" in avaiable_quote_assets and "USDT" in avaiable_quote_assets:
            result.append((base_asset, "USDT"))
            result.append((base_asset, "BTC"))

    return result


def aggregate(watchlist, start_timestamp, end_timestamp, date):
    result = []
    for (base_asset, quote_asset) in watchlist:
        symbol = base_asset + quote_asset
        print("fetch info: ", symbol)
        klines = binance.get_historical_klines(
            symbol=symbol,
            interval="1d",
            start_str=str(start_timestamp),
            end_str=str(end_timestamp),
            limit=99,
        )
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

    watchlist = fetch_watchlist()
    df = aggregate(watchlist, start_timestamp, end_timestamp, date)

    df_usdt = df[df.quote_asset == 'USDT']
    df_btc = df[df.quote_asset == 'BTC']

    btc_price = df_usdt[df_usdt.base_asset == 'BTC'].price.values[0]
    btc_volume = df_usdt[df_usdt.base_asset == 'BTC'].volume.values[0]
    eth_price = df_usdt[df_usdt.base_asset == 'ETH'].price.values[0]
    eth_volume = df_usdt[df_usdt.base_asset == 'ETH'].volume.values[0]

    total_volume_usdt = df_usdt.volume.sum()
    total_number_of_trades_usdt = df_usdt.number_of_trades.sum()
    df.loc[(df.quote_asset == 'USDT') & (df.base_asset == 'BTC'),
           'volume_ratio'] = None
    df.loc[(df.quote_asset == 'USDT') & (df.base_asset != 'BTC'),
           'volume_ratio'] = df_usdt.volume / total_volume_usdt - btc_volume

    df_usdt_active = df_usdt[(df_usdt.is_active) & (df_usdt.base_asset != 'BTC')]
    total_active_count_usdt = len(df_usdt_active)
    price_gc_rate_7_25_usdt = len(
        df_usdt_active[df_usdt_active.price_sma_7days > df_usdt_active.price_sma_25days]) / total_active_count_usdt
    price_gc_rate_25_99_usdt = len(
        df_usdt_active[df_usdt_active.price_sma_25days > df_usdt_active.price_sma_99days]) / total_active_count_usdt
    volume_gc_rate_7_25_usdt = len(
        df_usdt_active[df_usdt_active.volume_sma_7days > df_usdt_active.volume_sma_25days]) / total_active_count_usdt
    volume_gc_rate_25_99_usdt = len(
        df_usdt_active[df_usdt_active.volume_sma_25days > df_usdt_active.volume_sma_99days]) / total_active_count_usdt

    total_count = len(df_btc)
    total_volume_btc = df_btc.volume.sum()
    total_number_of_trades_btc = df_btc.number_of_trades.sum()
    df.loc[df.quote_asset == 'BTC', 'volume_ratio'] = df_btc.volume / total_volume_btc

    df_btc_active = df_btc[df_btc.is_active]
    total_active_count_btc = len(df_btc_active)
    price_gc_rate_7_25_btc = len(
        df_btc_active[df_btc_active.price_sma_7days > df_btc_active.price_sma_25days]) / total_active_count_btc
    price_gc_rate_25_99_btc = len(
        df_btc_active[df_btc_active.price_sma_25days > df_btc_active.price_sma_99days]) / total_active_count_btc
    volume_gc_rate_7_25_btc = len(
        df_btc_active[df_btc_active.volume_sma_7days > df_btc_active.volume_sma_25days]) / total_active_count_btc
    volume_gc_rate_25_99_btc = len(
        df_btc_active[df_btc_active.volume_sma_25days > df_btc_active.volume_sma_99days]) / total_active_count_btc

    summary_columns = [
        'date', 'btc_price', 'btc_volume', 'eth_price', 'eth_volume',
        'total_count_usdt', 'total_volume_usdt', 'total_number_of_trades_usdt',
        'price_gc_rate_7_25_usdt', 'price_gc_rate_25_99_usdt',
        'volume_gc_rate_7_25_usdt', 'volume_gc_rate_25_99_usdt',
        'total_count_btc', 'total_volume_btc', 'total_number_of_trades_btc',
        'price_gc_rate_7_25_btc', 'price_gc_rate_25_99_btc',
        'volume_gc_rate_7_25_btc', 'volume_gc_rate_25_99_btc'
    ]
    summary_df = pd.DataFrame([
        (date, btc_price, btc_volume, eth_price, eth_volume,
         total_count, total_volume_usdt, total_number_of_trades_usdt,
         price_gc_rate_7_25_usdt, price_gc_rate_25_99_usdt,
         volume_gc_rate_7_25_usdt, volume_gc_rate_25_99_usdt,
         total_volume_btc, total_number_of_trades_btc,
         price_gc_rate_7_25_btc, price_gc_rate_25_99_btc,
         volume_gc_rate_7_25_btc, volume_gc_rate_25_99_btc
         )], columns=summary_columns)

    summary_df = summary_df.astype({
        'date': 'datetime64[s]',
        'btc_price': 'float64',
        'btc_volume': 'float64',
        'eth_price': 'float64',
        'eth_volume': 'float64',
        'total_count': 'int',
        'total_volume_usdt': 'float64',
        'total_number_of_trades_usdt': 'int',
        'price_gc_rate_7_25_usdt': 'float64',
        'price_gc_rate_25_99_usdt': 'float64',
        'volume_gc_rate_7_25_usdt': 'float64',
        'volume_gc_rate_25_99_usdt': 'float64',
        'total_volume_btc': 'float64',
        'total_number_of_trades_btc': 'int',
        'price_gc_rate_7_25_btc': 'float64',
        'price_gc_rate_25_99_btc': 'float64',
        'volume_gc_rate_7_25_btc': 'float64',
        'volume_gc_rate_25_99_btc': 'float64'
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
    print("Summary has been saved!")
