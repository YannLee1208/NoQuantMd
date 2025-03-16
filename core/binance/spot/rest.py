"""
coding=utf-8
@File   : rest
@Author : LiHan
@Time   : 3/16/25:12:50 PM
"""
import time
from datetime import datetime, timezone
from enum import Enum
from loguru import logger
import pandas as pd

from core.constant.object import Exchange, Interval
from core.template.rest import RestClient, RestRequest
from core.utils.constant import Env

REST_API_DATA_BASE_URL: dict[Env, str] = {
    Env.PROD: "https://data-api.binance.vision",
    Env.TEST: "https://testnet.binance.vision"
}

API_LIMIT_ONE_TIME = 1000


class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2


class BinanceSpotDataRestAPi(RestClient):

    def __init__(self):
        super(BinanceSpotDataRestAPi, self).__init__()
        self.gateway_name = "binance_spot_data_rest_api"
        self.time_offset = 0  # 服务器时间偏移, 毫秒

    def connect(
            self, env: Env, proxy_host: str, proxy_port: int):
        url_base = REST_API_DATA_BASE_URL[env]
        self.init(
            url_base=url_base,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
        )
        self.start()
        self.query_time()

    def query_time(self):
        """
        查询服务器时间
        """
        logger.info("Querying server time")
        data: dict = {"security": Security.NONE}
        path: str = "/api/v3/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_kline(self, symbol: str, interval: Interval, start_timestamp: int, end_timestamp: int) -> pd.DataFrame:
        """
        查询K线数据
        :param symbol: 交易对
        :param interval: K线周期
        :param start_timestamp: 开始时间
        :param end_timestamp: 结束时间
        """
        current_start = start_timestamp
        all_data = []
        while current_start < end_timestamp:
            df = self._query_klines(symbol, interval, current_start, end_timestamp)
            if df.empty:
                break
            df["Symbol"] = symbol
            df["Exchange"] = Exchange.BINANCE.value
            df["Interval"] = interval.value
            df["OpenInterest"] = 0
            all_data.append(df)

            # 获取最后一条数据的 CloseTimestamp
            last_close_ts = df.iloc[-1]["CloseTime"]
            if last_close_ts >= end_timestamp:
                break
            # 避免重复数据，下一次从 last_close_ts + 1 毫秒开始
            current_start = last_close_ts + 1

        if all_data:
            res = pd.concat(all_data, ignore_index=True)
            res.drop_duplicates(subset=["ExchangeTime"], keep="first", inplace=True)
            res.drop(columns=["CloseTime"], inplace=True)
            return res
        else:
            return pd.DataFrame()

    def _query_klines(self, symbol: str, interval: Interval, start_timestamp: int, end_timestamp: int,
                      limit=API_LIMIT_ONE_TIME) -> pd.DataFrame:
        """
        获取K线数据，API文档：https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
        :param symbol: 交易Symbol，如BTCUSDT
        :param interval: k线周期
        :param start_timestamp: 开始时间，timestamp格式
        :param end_timestamp: 结束时间, timestamp格式
        :param limit: 单次请求的K线数量
        :return: K线数据, DataFrame格式
        """
        path = "/api/v3/klines"

        params = {
            'symbol': symbol,
            'interval': interval.value,
            'startTime': start_timestamp,
            'endTime': end_timestamp,
            'limit': limit
        }

        response = self.request(
            "GET",
            path,
            params=params
        )

        data = response.json()
        columns = ['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime',
                   'QuoteAssetVolume', 'NumberOfTrades', 'TakerBuyBaseAssetVolume',
                   'TakerBuyQuoteAssetVolume', 'Ignore']

        df = pd.DataFrame(data, columns=columns)

        # 转换时间戳为可读格式
        df["LocalTime"] = int(datetime.now().timestamp() * 1000)
        df.rename({
            "OpenTime": "ExchangeTime",
            "QuoteAssetVolume": "Turnover"
        }, axis=1, inplace=True)
        df.drop(columns=["Ignore"], inplace=True)

        return df

    def on_query_time(self, data: dict, request: RestRequest):
        """
        服务器时间查询回调函数
        """
        logger.info(f"Server time returned: {data}")
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

        logger.info(f"Server time updated, local offset: {self.time_offset}ms")


if __name__ == '__main__':
    rest_api = BinanceSpotDataRestAPi()

    rest_api.connect(Env.PROD, "", 0)

    # time.sleep(10)
    print(f"Time offset: {rest_api.time_offset}")

    day = "2025-03-15"
    symbol = "BTCUSDT"
    interval = Interval.SECOND

    start_timestamp = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_timestamp = start_timestamp + 24 * 60 * 60 * 1000 - 1  # 一天的时间戳范围

    res = rest_api.query_kline(symbol, interval, start_timestamp, end_timestamp)
    print(res)
