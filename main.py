import os
from datetime import datetime, timedelta, timezone

from binance import Client
from google.cloud import bigquery

from binance_data_aggregator import BinanceDataAggregator
from crypto_data_analyzer import CryptoDataAnalyzer
from data_store import KlineStore
from discord_notifier import DiscordNotifier


class Config:
    binance_api_key = os.environ.get("BINANCE_API_KEY")
    binance_api_secret = os.environ.get("BINANCE_API_SECRET")
    output_table = os.environ.get("BIGQUERY_OUTPUT_TABLE")
    discord_webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")


bq = bigquery.Client()
binance = Client(api_key=Config.binance_api_key, api_secret=Config.binance_api_secret)
aggregator = BinanceDataAggregator(binance)
store = KlineStore(client=bq, table=Config.output_table)
analyzer = CryptoDataAnalyzer()
notifier = DiscordNotifier(webhook_url=Config.discord_webhook_url)


def execute(request, context):
    dt = datetime.now(tz=timezone(timedelta(hours=+9), "JST")).replace(
        minute=0, second=0, microsecond=0
    )

    klines = aggregator.aggregate_hourly_data(dt)
    error = store.save_klines(klines)

    if error:
        print(error)
        return

    stored_klines = store.fetch_klines(dt)
    summary = analyzer.analyze(stored_klines)
    if summary:
        notifier.notify(summary)

    return klines
