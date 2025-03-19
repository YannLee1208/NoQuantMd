"""
coding=utf-8
@File   : rest
@Author : LiHan
@Time   : 3/16/25:12:50 PM
"""
import time
from datetime import datetime

import pandas as pd

from core.utils.constant import Security
from external.common.constant import API_LIMIT_ONE_TIME
from external.common.env import REST_API_DATA_BASE_URL
from external.common.object import Exchange, Interval
from external.rest.rest import RestClient, RestRequest
from external.utils.log import logger


class BinanceSpotDataRestAPi(RestClient):

    def __init__(self):
        super(BinanceSpotDataRestAPi, self).__init__()
        self.gateway_name = "binance_spot_data_rest_api"
        self.time_offset = 0  # 服务器时间偏移, 毫秒

    def connect(
            self, proxy_host: str, proxy_port: int):
        self.init(
            url_base=REST_API_DATA_BASE_URL,
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
        :param symbol: 交易Symbol，如BTCUSDT
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

    def query_trading_day_ticker(self, symbol: str, ticker_type: str = "FULL") -> pd.DataFrame:
        """
        查询交易日价格统计
        :param symbol: 交易Symbol，如BTCUSDT
        :param time_zone: 时区，默认为"0"(UTC)
        :param ticker_type: 类型，FULL或MINI，默认为FULL
        :return: DataFrame
        """
        df = self._query_trading_day_ticker(symbol, ticker_type)
        if not df.empty:
            df["Symbol"] = symbol
            df["Exchange"] = Exchange.BINANCE.value
            return df
        else:
            return pd.DataFrame()

    def query_agg_trades(self, symbol: str, start_timestamp: int, end_timestamp: int) -> pd.DataFrame:
        """
        查询aggregated trades
        :param symbol: 交易Symbol，如BTCUSDT
        :param start_timestamp: 开始时间
        :param end_timestamp: 结束时间
        :return: DataFrame
        """
        current_start = start_timestamp
        all_data = []
        while current_start < end_timestamp:
            df = self._query_agg_trades(symbol, current_start, end_timestamp)
            if df.empty:
                break

            df["Symbol"] = symbol
            df["Exchange"] = Exchange.BINANCE.value
            all_data.append(df)

            # 获取最后一条数据的时间戳
            last_timestamp = df.iloc[-1]["Timestamp"]
            if last_timestamp >= end_timestamp:
                break
            # 避免重复数据，下一次从 last_timestamp + 1 毫秒开始
            current_start = last_timestamp + 1

        if all_data:
            res = pd.concat(all_data, ignore_index=True)
            res.drop_duplicates(subset=["AggTradeId"], keep="first", inplace=True)
            return res
        else:
            return pd.DataFrame()

    def query_historical_trades(self, symbol: str, start_timestamp: int, end_timestamp: int) -> pd.DataFrame:
        """
        查询历史交易数据
        :param symbol: 交易Symbol，如BTCUSDT
        :param start_timestamp: 开始时间
        :param end_timestamp: 结束时间
        :return: DataFrame
        """
        current_id = None
        all_data = []

        # 初始获取最新交易
        df = self._query_historical_trades(symbol)
        if df.empty:
            return pd.DataFrame()

        # 筛选时间范围内的交易
        df = df[df["Time"].between(start_timestamp, end_timestamp)]
        if not df.empty:
            df["Symbol"] = symbol
            df["Exchange"] = Exchange.BINANCE.value
            all_data.append(df)
            current_id = df["Id"].min() - 1

        # 继续获取更早的交易
        while current_id and current_id > 0:
            df = self._query_historical_trades(symbol, from_id=current_id)
            if df.empty:
                break

            # 筛选时间范围内的交易
            df = df[df["Time"].between(start_timestamp, end_timestamp)]
            if df.empty:
                break

            df["Symbol"] = symbol
            df["Exchange"] = Exchange.BINANCE.value
            all_data.append(df)

            # 检查是否已经获取到足够早的交易
            min_time = df["Time"].min()
            if min_time < start_timestamp:
                break

            current_id = df["Id"].min() - 1

        if all_data:
            res = pd.concat(all_data, ignore_index=True)
            res.drop_duplicates(subset=["Id"], keep="first", inplace=True)
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

    def _query_agg_trades(self, symbol: str, start_timestamp: int, end_timestamp: int,
                          limit=API_LIMIT_ONE_TIME) -> pd.DataFrame:
        """
        获取聚合交易数据
        :param symbol: 交易Symbol，如BTCUSDT
        :param start_timestamp: 开始时间，毫秒时间戳
        :param end_timestamp: 结束时间，毫秒时间戳
        :param limit: 单次请求的数据数量，默认1000
        :return: 聚合交易数据，DataFrame格式
        """
        path = "/api/v3/aggTrades"

        params = {
            'symbol': symbol,
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
        if not data:
            return pd.DataFrame()

        # 将API返回的数据转换为DataFrame
        df = pd.DataFrame(data)

        # 重命名列名以提高可读性
        df.rename(columns={
            "a": "AggTradeId",
            "p": "Price",
            "q": "Volume",
            "f": "FirstTradeId",
            "l": "LastTradeId",
            "T": "TradeTimestamp",
            "m": "IsBuyerMaker",
            "M": "IsBestPriceMatch"
        }, inplace=True)

        # 转换数据类型
        df["Price"] = df["Price"].astype(float)
        df["Quantity"] = df["Quantity"].astype(float)
        df["Turnover"] = df["Price"] * df["Quantity"]
        df["LocalTime"] = int(datetime.now().timestamp() * 1000)

        return df

    # FIXME: 未完成
    def _query_historical_trades(self, symbol: str, from_id: int = None, limit=API_LIMIT_ONE_TIME) -> pd.DataFrame:
        """
        获取历史交易数据
        :param symbol: 交易Symbol，如BTCUSDT
        :param from_id: 从哪个交易ID开始获取
        :param limit: 单次请求的数据数量，默认1000
        :return: 历史交易数据，DataFrame格式
        """
        path = "/api/v3/historicalTrades"

        params = {
            'symbol': symbol,
            'limit': limit
        }

        if from_id is not None:
            params['fromId'] = from_id
        else:
            params['fromId'] = 0

        response = self.request(
            "GET",
            path,
            params=params
        )

        data = response.json()
        if not data:
            return pd.DataFrame()

        # 将API返回的数据转换为DataFrame
        df = pd.DataFrame(data)

        # 重命名列名以提高可读性
        df.rename(columns={
            "id": "Id",
            "price": "Price",
            "qty": "Quantity",
            "quoteQty": "QuoteQuantity",
            "time": "Time",
            "isBuyerMaker": "IsBuyerMaker",
            "isBestMatch": "IsBestMatch"
        }, inplace=True)

        # 转换数据类型
        df["Price"] = df["Price"].astype(float)
        df["Quantity"] = df["Quantity"].astype(float)
        df["QuoteQuantity"] = df["QuoteQuantity"].astype(float)
        df["LocalTime"] = int(datetime.now().timestamp() * 1000)

        return df

    def _query_trading_day_ticker(self, symbol: str, ticker_type: str = "FULL") -> pd.DataFrame:
        """
        获取交易日价格统计数据
        :param symbol: 交易Symbol，如BTCUSDT
        :param time_zone: 时区，默认为"0"(UTC)
        :param ticker_type: 类型，FULL或MINI，默认为FULL
        :return: 交易日价格统计数据，DataFrame格式
        """
        path = "/api/v3/ticker/tradingDay"

        params = {
            'symbol': symbol,
            'type': ticker_type
        }

        response = self.request(
            "GET",
            path,
            params=params
        )

        data = response.json()
        if not data:
            return pd.DataFrame()

        # 将API返回的数据转换为DataFrame
        if isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            df = pd.DataFrame(data)

        # 转换数据类型
        numeric_columns = ["priceChange", "priceChangePercent", "weightedAvgPrice",
                           "openPrice", "highPrice", "lowPrice", "lastPrice",
                           "volume", "quoteVolume"]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # 添加本地时间戳
        df["LocalTime"] = int(datetime.now().timestamp() * 1000)

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
