from datetime import datetime, timedelta

from binance import Client


class BinanceDataAggregator:
    def __init__(self, binance_client: Client):
        self.binance_client = binance_client

    def get_watchlist(self) -> list[tuple[str, str]]:
        res = self.binance_client.get_exchange_info()
        symbols = res["symbols"]
        dict = {}

        for s in symbols:
            base_asset = s["baseAsset"]
            quote_asset = s["quoteAsset"]
            if base_asset not in dict:
                dict[base_asset] = []
            dict[base_asset].append(quote_asset)

        result = [("BTC", "USDT")]
        for key in dict:
            base_assets = dict[key]
            if "USDT" in base_assets and "BTC" in base_assets:
                result.append((key, "USDT"), (key, "BTC"))
        return result

    def get_historical_kline(
        self, base_asset: str, quote_asset: str, open_time: datetime
    ) -> dict:
        symbol = base_asset + quote_asset
        start_timestamp = datetime.timestamp(open_time)
        end_timestamp = datetime.timestamp(open_time + timedelta(hours=1))

        klines = self.binance_client.get_historical_klines(
            symbol=symbol,
            interval="1h",
            start_str=str(start_timestamp),
            end_str=str(end_timestamp),
            limit=1,
        )

        [
            open_time,
            open_price,
            high,
            low,
            close_price,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trade,
            taker_buy_base_asset_volume,
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
        row["volume"] = float(volume)
        row["quote_asset_volume"] = float(quote_asset_volume)
        row["taker_buy_base_asset_volume"] = float(taker_buy_base_asset_volume)
        row["taker_buy_quote_asset_volume"] = float(taker_buy_quote_asset_volume)

        return row
