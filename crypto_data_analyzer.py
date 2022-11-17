import pandas as pd

from data import Kline


class CryptoDataAnalyzer:
    ranking_count = 10

    def analyze(self, klines: list[list[Kline]]):
        if len(klines) <= 1:
            return
        if not klines[0] or not klines[1]:
            return

        latest = pd.DataFrame([row.to_dict() for row in klines[0]])
        previous = pd.DataFrame([row.to_dict() for row in klines[1]])

        df = latest.join(previous, rsuffix="_prev")

        df["price_change"] = df.close_price - df.close_price_prev
        df["price_change_rate"] = df.price_change / df.close_price_prev
        df["volume_change"] = df.quote_asset_volume - df.quote_asset_volume_prev
        df["volume_change_rate"] = df.volume_change / df.quote_asset_volume_prev

        df = df[
            [
                "base_asset",
                "quote_asset",
                "close_price",
                "quote_asset_volume",
                "quote_asset_volume_prev",
                "price_change",
                "price_change_rate",
                "volume_change",
                "volume_change_rate",
            ]
        ]

        df_usdt = df[df.quote_asset == "USDT"]
        df_btc = df[df.quote_asset == "BTC"]

        asset_count = len(df_usdt.index)
        price_up_asset_count = len(df_usdt[df_usdt.price_change_rate > 0].index)
        volume_up_asset_count = len(df_usdt[df_usdt.volume_change_rate > 0].index)
        price_up_ratio = price_up_asset_count / asset_count
        volume_up_ratio = volume_up_asset_count / asset_count
        total_volume = int(df_usdt.quote_asset_volume.sum())
        total_volume_prev = int(df_usdt.quote_asset_volume_prev.sum())
        total_volume_change_rate = (
            total_volume - total_volume_prev
        ) / total_volume_prev

        btc_data = df_usdt[df_usdt.base_asset == "BTC"].values.tolist()
        if len(btc_data) > 0:
            btc_data = btc_data[0]
        else:
            btc_data = None

        gainers = (
            df_usdt[df_usdt.quote_asset_volume > 2000]
            .sort_values(by="price_change_rate", ascending=False)
            .head(self.ranking_count)
        )

        losers = (
            df_usdt[df_usdt.quote_asset_volume > 2000]
            .sort_values(by="price_change_rate")
            .head(self.ranking_count)
        )

        gainers_volume = (
            df_usdt[df_usdt.quote_asset_volume > 2000]
            .sort_values(by="volume_change_rate", ascending=False)
            .head(self.ranking_count)
        )

        btc_gainers = (
            df_btc[df_btc.quote_asset_volume > 1]
            .sort_values(by="price_change_rate", ascending=False)
            .head(self.ranking_count)
        )

        btc_losers = (
            df_btc[df_btc.quote_asset_volume > 1]
            .sort_values(by="price_change_rate")
            .head(self.ranking_count)
        )

        return {
            "total_asset_count": asset_count,
            "total_volume": total_volume,
            "total_volume_change_rate": total_volume_change_rate,
            "price_up_ratio": price_up_ratio,
            "volume_up_ratio": volume_up_ratio,
            "btc_data": {
                "price": btc_data[2],
                "volume": btc_data[3],
                "price_change_ratio": btc_data[6],
                "volume_change_ratio": btc_data[8],
            }
            if btc_data
            else None,
            "gainers": gainers.values.tolist(),
            "losers": losers.values.tolist(),
            "gainers_volume": gainers_volume.values.tolist(),
            "btc_gainers": btc_gainers.values.tolist(),
            "btc_losers": btc_losers.values.tolist(),
        }
