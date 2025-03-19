"""
coding=utf-8
@File   : constant
@Author : LiHan
@Time   : 3/12/25:2:49 PM
"""
from enum import Enum

WEBSOCKET_RECEIVE_TIMEOUT_SECOND = 24 * 60 * 60


class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2
