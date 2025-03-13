"""
coding=utf-8
@File   : constant
@Author : LiHan
@Time   : 3/12/25:2:58 PM
"""

from enum import Enum


class Exchange(Enum):
    BINANCE = "BINANCE"


class Product(Enum):
    STOCK = "股票"
    SPOT = "现货"


class OptionType(Enum):
    """
    期权类型
    """
    CALL = "看涨期权"
    PUT = "看跌期权"
