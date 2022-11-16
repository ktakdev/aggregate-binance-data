import json
import math
from datetime import datetime, timedelta

import requests


class DiscordNotifier:
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def notify(self, summary):
        requests.post(
            self.webhook_url,
            json.dumps({"embeds": self.create_notification_embeds(summary)}),
            headers={"Content-Type": "application/json"},
        )

    def format_ratio(self, ratio):
        return f"{math.floor(ratio * 10000)/100}%"

    def format_dollar(self, dollar_price):
        return "$" + "{:,}".format(math.floor(dollar_price))

    def create_notification_embeds(self, summary):
        total_asset_count = summary["total_asset_count"]
        price_up_ratio = summary["price_up_ratio"]
        volume_up_ratio = summary["volume_up_ratio"]
        price_up_asset = int(total_asset_count * price_up_ratio)
        volume_up_asset = int(total_asset_count * volume_up_ratio)

        open_time = datetime.fromtimestamp(summary["open_time"])
        close_time = open_time + timedelta(hours=1)
        btc_data = summary["btc_data"]

        gainers = summary["gainers"]
        gainers_volume = summary["gainers_volume"]
        losers = summary["losers"]
        btc_gainers = summary["btc_gainers"]
        btc_losers = summary["btc_losers"]

        summry_embeds = {
            "title": "サマリー",
            "fields": [
                {
                    "name": "集計期間",
                    "value": open_time.strftime("%m/%d %H:%M")
                    + " - "
                    + close_time.strftime("%m/%d %H:%M"),
                },
                {"name": "集計銘柄数", "value": total_asset_count},
                {
                    "name": "BTC価格",
                    "value": f"{btc_data[2]} ({self.format_ratio(btc_data[5])})\n"
                    + "[チャートを見る](https://www.binance.com/en/trade/BTC_USDT)"
                    if btc_data
                    else "None",
                },
                {
                    "name": "BTC出来高",
                    "value": self.format_dollar(btc_data[3])
                    + "("
                    + self.format_ratio(btc_data[7])
                    + ")"
                    if btc_data
                    else "None",
                },
                {
                    "name": "上昇銘柄率",
                    "value": self.format_ratio(price_up_ratio)
                    + f" ({price_up_asset}/{total_asset_count})",
                },
                {
                    "name": "出来高上昇銘柄率",
                    "value": self.format_ratio(volume_up_ratio)
                    + f" ({volume_up_asset}/{total_asset_count})",
                },
            ],
        }

        gainers_embeds = {
            "title": ":chart_with_upwards_trend: 上昇率ランキング(USDT建て)",
            "fields": [
                {
                    "name": row[0],
                    "value": self.format_ratio(row[5])
                    + f" (${row[2]})\n"
                    + f"[チャートを見る](https://www.binance.com/en/trade/{row[0]}_{row[1]})",
                }
                for row in gainers
            ],
        }

        gainers_btc_embeds = {
            "title": ":chart_with_upwards_trend: 上昇率ランキング(BTC建て)",
            "fields": [
                {
                    "name": row[0],
                    "value": self.format_ratio(row[5])
                    + f" ({row[2]} BTC)\n"
                    + f"[チャートを見る](https://www.binance.com/en/trade/{row[0]}_{row[1]})",
                }
                for row in btc_gainers
            ],
        }

        volume_embeds = {
            "title": ":zap: 出来高増加率ランキング(USDT建て)",
            "fields": [
                {
                    "name": row[0],
                    "value": self.format_ratio(row[7])
                    + f" ({self.format_dollar(row[3])})\n"
                    + f"[チャートを見る](https://www.binance.com/en/trade/{row[0]}_{row[1]})",
                }
                for row in gainers_volume
            ],
        }

        losers_embeds = {
            "title": ":chart_with_downwards_trend: 下降率ランキング(USDT建て)",
            "fields": [
                {
                    "name": row[0],
                    "value": self.format_ratio(row[5])
                    + f" (${row[2]})\n"
                    + f"[チャートを見る](https://www.binance.com/en/trade/{row[0]}_{row[1]})",
                }
                for row in losers
            ],
        }

        losers_btc_embeds = {
            "title": ":chart_with_downwards_trend: 下降率ランキング(BTC建て)",
            "fields": [
                {
                    "name": row[0],
                    "value": self.format_ratio(row[5])
                    + f" ({row[2]} BTC)\n"
                    + f"[チャートを見る](https://www.binance.com/en/trade/{row[0]}_{row[1]})",
                }
                for row in btc_losers
            ],
        }

        return [
            summry_embeds,
            gainers_embeds,
            gainers_btc_embeds,
            volume_embeds,
            losers_embeds,
            losers_btc_embeds,
        ]
