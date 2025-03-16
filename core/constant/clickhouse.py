"""
coding=utf-8
@File   : clickhouse
@Author : LiHan
@Time   : 3/16/25:3:49 PM
"""

DATABASE_KLINE = "kline"

INTERVAL_TABLE_MAPPING = {
    "1h": "one_hour_binance",
    "1d": "daily_binance",
    "1m": "one_minute_binance",
    "1s": "one_second_binance"
}
