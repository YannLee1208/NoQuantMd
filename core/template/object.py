"""
coding=utf-8
@File   : object
@Author : LiHan
@Time   : 3/12/25:2:57 PM
"""
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from core.constant.object import Exchange, Product, OptionType, Interval


@dataclass
class BaseData:
    """
    所有数据类的基类，需要gateway_name来标识数据来自哪个gateway
    """

    gateway_name: str

    extra: Optional[dict] = field(default=None, init=False)


@dataclass
class TickData(BaseData):
    symbol: str
    exchange: Exchange
    exchange_time: datetime.timestamp
    local_time: datetime.timestamp

    name: str = ""
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    last_price: float = 0
    last_volume: float = 0
    limit_up: float = 0
    limit_down: float = 0

    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    pre_close: float = 0

    bid_price_1: float = 0
    bid_price_2: float = 0
    bid_price_3: float = 0
    bid_price_4: float = 0
    bid_price_5: float = 0

    ask_price_1: float = 0
    ask_price_2: float = 0
    ask_price_3: float = 0
    ask_price_4: float = 0
    ask_price_5: float = 0

    bid_volume_1: float = 0
    bid_volume_2: float = 0
    bid_volume_3: float = 0
    bid_volume_4: float = 0
    bid_volume_5: float = 0

    ask_volume_1: float = 0
    ask_volume_2: float = 0
    ask_volume_3: float = 0
    ask_volume_4: float = 0
    ask_volume_5: float = 0

    def __post_init__(self) -> None:
        self.virtual_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class KLineData(BaseData):
    """
    K线数据类
    """

    symbol: str
    exchange: Exchange
    exchange_time: datetime.timestamp
    local_time: datetime.timestamp

    interval: Interval = None
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0  # 持仓量，对于spot market，该值为0
    open: float = 0
    high: float = 0
    low: float = 0
    close: float = 0
    taker_buy_base_asset_volume: float = 0  # 买入交易量
    taker_buy_quote_asset_volume: float = 0  # 买入交易额
    number_of_trades: int = 0  # 交易次数

    def __post_init__(self) -> None:
        """"""
        self.virtual_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class AggTradesData(BaseData):
    """
    Aggregate trades data from Binance.
    """
    symbol: str
    exchange: Exchange
    local_time: int = 0  # Local system timestamp

    agg_trade_id: int = 0  # Aggregate trade ID
    price: float = 0  # Trade price
    volume: float = 0  # Trade quantity
    turnover: float = 0  # Total value (price * quantity)
    first_trade_id: int = 0  # First trade ID in the aggregate
    last_trade_id: int = 0  # Last trade ID in the aggregate
    trade_timestamp: int = 0  # Trade timestamp (milliseconds)
    is_buyer_maker: bool = False  # Was the buyer the maker?
    is_best_price_match: bool = False  # Was trade executed at the best price?

    def __post_init__(self) -> None:
        self.virtual_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class SubscribeRequest:
    """
    Request sending to specific gateway for subscribing tick data update.
    """

    symbol: str
    exchange: Exchange

    def __post_init__(self) -> None:
        """"""
        self.virtual_symbol: str = f"{self.symbol}.{self.exchange.value}"
