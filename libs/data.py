from datetime import datetime


class Kline:
    def __init__(
        self,
        base_asset: str,
        quote_asset: str,
        open_time: float,
        close_time: float,
        open_price: float,
        high: float,
        low: float,
        close_price: float,
        number_of_trade: int,
        volume: float,
        quote_asset_volume: float,
        taker_buy_base_asset_volume: float,
        taker_buy_quote_asset_volume: float,
    ) -> None:
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.open_time = open_time
        self.close_time = close_time
        self.open_price = open_price
        self.high = high
        self.low = low
        self.close_price = close_price
        self.number_of_trade = number_of_trade
        self.volume = volume
        self.quote_asset_volume = quote_asset_volume
        self.taker_buy_base_asset_volume = taker_buy_base_asset_volume
        self.taker_buy_quote_asset_volume = taker_buy_quote_asset_volume

    def to_dict(self) -> dict:
        return vars(self)

    def __repr__(self) -> str:
        return str(self.to_dict())

    def __str__(self):
        return str(self.to_dict())

    @staticmethod
    def from_tuple(tuple: tuple):
        (
            base_asset,
            quote_asset,
            open_time,
            close_time,
            open_price,
            high,
            low,
            close_price,
            number_of_trade,
            volume,
            quote_asset_volume,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
        ) = tuple

        return Kline(
            base_asset=base_asset,
            quote_asset=quote_asset,
            open_time=datetime.timestamp(open_time),
            close_time=datetime.timestamp(close_time),
            open_price=open_price,
            high=high,
            low=low,
            close_price=close_price,
            number_of_trade=number_of_trade,
            volume=volume,
            quote_asset_volume=quote_asset_volume,
            taker_buy_base_asset_volume=taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume=taker_buy_quote_asset_volume,
        )
