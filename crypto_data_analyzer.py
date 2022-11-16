import pandas as pd


class CryptoDataAnalyzer:

    data: list[pd.DataFrame] = []
    max_stored_data_count = 24
    ranking_count = 10

    def add_hourly_data(self, rows: list[list]):
        df = pd.DataFrame(rows)
        df.set_index(keys=["base_asset", "quote_asset"])
        self.data.insert(0, df)
        if len(self.data) > self.max_stored_data_count:
            self.data.pop()

    def analyze(self, rows: list[list]):
        if len(self.data) <= 1:
            return

        latest = self.data[0]
        previous = self.data[1]

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

        btc_data = df_usdt[df_usdt.base_asset == "BTC"]

        gainers = df_usdt.sort_values(by="price_change_rate", ascending=False).head(
            self.ranking_count
        )

        losers = df_usdt.sort_values(by="price_change_rate").head(self.ranking_count)

        gainers_volume = df_usdt.sort_values(
            by="volume_change_rate", ascending=False
        ).head(self.ranking_count)

        btc_gainers = df_btc.sort_values(by="price_change_rate", ascending=False).head(
            self.ranking_count
        )

        btc_losers = df_btc.sort_values(by="price_change_rate").head(self.ranking_count)

        return {
            "total_asset_count": asset_count,
            "price_up_ratio": price_up_ratio,
            "volume_up_ratio": volume_up_ratio,
            "btc_data": btc_data.values.tolist(),
            "gainers": gainers.values.tolist(),
            "losers": losers.values.tolist(),
            "gainers_volume": gainers_volume.values.tolist(),
            "btc_gainers": btc_gainers.values.tolist(),
            "btc_losers": btc_losers.values.tolist(),
        }
