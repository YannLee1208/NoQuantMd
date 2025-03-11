import pandas as pd
import requests

from constant.api import BASE_URL, KLINE_URL, API_LIMIT_ONE_TIME
from utils.common import get_requests_session


def _get_klines(symbol: str, interval: str, start_time: int, end_time: int, limit=API_LIMIT_ONE_TIME) -> pd.DataFrame:
    """
    获取K线数据，API文档：https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :param start_time: 开始时间，timestamp格式
    :param end_time: 结束时间, timestamp格式
    :param limit: 单次请求的K线数量
    :return: K线数据, DataFrame格式
    """
    url = BASE_URL + KLINE_URL
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit
    }
    # response = requests.get(url, params=params, timeout=10)
    session = get_requests_session()
    response = session.get(url, params=params, timeout=10)
    data = response.json()
    columns = ['OpenTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime',
               'QuoteAssetVolume', 'NumberOfTrades', 'TakerBuyBaseAssetVolume',
               'TakerBuyQuoteAssetVolume', 'Ignore']
    df = pd.DataFrame(data, columns=columns)

    df["Symbol"] = symbol
    # 转换时间戳为可读格式
    df["OpenTimestamp"] = df["OpenTime"]
    df["CloseTimestamp"] = df["CloseTime"]
    df['OpenTime'] = pd.to_datetime(df['OpenTime'], unit='ms', utc=True)
    df['CloseTime'] = pd.to_datetime(df['CloseTime'], unit='ms', utc=True)
    df.drop(columns=["Ignore"], inplace=True)
    return df


def get_klines(symbol: str, interval: str, start_time: int, end_time: int) -> pd.DataFrame:
    """
    分页获取所有K线数据，直至 end_time
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :param start_time: 开始时间，timestamp格式
    :param end_time: 结束时间, timestamp格式
    """
    all_data = []
    current_start = start_time
    while current_start < end_time:
        df = _get_klines(symbol, interval, current_start, end_time)
        if df.empty:
            break
        all_data.append(df)

        # 获取最后一条数据的 CloseTimestamp
        last_close_ts = df.iloc[-1]["CloseTimestamp"]
        if last_close_ts >= end_time:
            break
        # 避免重复数据，下一次从 last_close_ts + 1 毫秒开始
        current_start = last_close_ts + 1

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()