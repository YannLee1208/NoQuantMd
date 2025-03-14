"""
coding=utf-8
@File   : object
@Author : LiHan
@Time   : 3/12/25:2:57 PM
"""
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from core.template.constant import Exchange, Product, OptionType, Interval


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
    Candlestick data of a certain trading period.
    """

    symbol: str
    exchange: Exchange
    exchange_time: datetime.timestamp
    local_time: datetime.timestamp

    interval: Interval = None
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0

    def __post_init__(self) -> None:
        """"""
        self.virtual_symbol: str = f"{self.symbol}.{self.exchange.value}"


@dataclass
class ContractData(BaseData):
    """
    Contract data contains basic information about each contract traded.
    """

    symbol: str
    exchange: Exchange
    name: str
    product: Product
    size: float
    price_tick: float

    min_volume: float = 1  # minimum order volume
    max_volume: float = None  # maximum order volume
    stop_supported: bool = False  # whether server supports stop order
    net_position: bool = False  # whether gateway uses net position volume
    history_data: bool = False  # whether gateway provides bar history data

    option_strike: float = 0
    option_underlying: str = ""  # vt_symbol of underlying contract
    option_type: OptionType = None
    option_listed: datetime = None
    option_expiry: datetime = None
    option_portfolio: str = ""
    option_index: str = ""  # for identifying options with same strike price

    def __post_init__(self) -> None:
        """"""
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
