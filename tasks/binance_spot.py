import os
from datetime import datetime, timezone

from core.binance.spot.rest import BinanceSpotDataRestAPi
from core.utils.constant import Env
from external.config import global_config
from external.object import Interval, Exchange
from external.utils.date import cal_date_interval
from external.utils.log import logger

KEY_DATA_STORE = "data_store_path"


def fetch_all_klines(start_trading_day: str, end_trading_day: str,
                     symbol: str, interval: Interval, store_dir: str):
    """
    获取指定时间范围内的所有K线数据
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :param store_dir: 存储目录
    :return: None
    """
    data_dir = os.path.join(store_dir, Exchange.BINANCE.value, "spot", symbol, interval.value)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    rest_api = BinanceSpotDataRestAPi()
    rest_api.connect(Env.PROD, "", 0)

    days = cal_date_interval(start_trading_day, end_trading_day)
    for day in days:
        logger.info(f"获取{day}的K线数据")
        start_timestamp = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_timestamp = start_timestamp + 24 * 60 * 60 * 1000 - 1
        klines = rest_api.query_kline(symbol, interval, start_timestamp, end_timestamp)

        if klines.empty:
            logger.warning(f"{day}没有获取到K线数据")
            break
        klines["TradingDay"] = day
        # 保存到CSV文件
        file_path = os.path.join(data_dir, f"{day}_klines.csv")
        logger.info(f"{day}的K线数据大小: {klines.shape}")
        klines.to_csv(file_path, index=False)


def fetch_agg_traders(start_trading_day: str, end_trading_day: str,
                      symbol: str, store_dir: str):
    """
    获取指定时间范围内的所有K线数据
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :param store_dir: 存储目录
    :return: None
    """
    data_dir = os.path.join(store_dir, Exchange.BINANCE.value, "spot", symbol, "agg_traders")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    rest_api = BinanceSpotDataRestAPi()
    rest_api.connect(Env.PROD, "", 0)

    days = cal_date_interval(start_trading_day, end_trading_day)
    for day in days:
        logger.info(f"获取{day}的数据")
        start_timestamp = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_timestamp = start_timestamp + 24 * 60 * 60 * 1000 - 1
        klines = rest_api.query_agg_trades(symbol, start_timestamp, end_timestamp)

        if klines.empty:
            logger.warning(f"{day}没有获取到K线数据")
            break
        klines["TradingDay"] = day
        # 保存到CSV文件
        file_path = os.path.join(data_dir, f"{day}_agg_traders.csv")
        logger.info(f"{day}的数据大小: {klines.shape}")
        klines.to_csv(file_path, index=False)


def fetch_trading_day_ticker(trading_day: str, symbol: str, store_dir: str, ticker_type: str = "FULL"):
    """
    获取指定交易日的价格统计数据
    :param trading_day: 交易日
    :param symbol: 交易Symbol，如BTCUSDT
    :param time_zone: 时区，默认为"0"(UTC)
    :param store_dir: 存储目录
    :param ticker_type: 类型，FULL或MINI，默认为FULL
    :return: None
    """
    data_dir = os.path.join(store_dir, Exchange.BINANCE.value, "spot", symbol, "trading_day_ticker")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    rest_api = BinanceSpotDataRestAPi()
    rest_api.connect(Env.PROD, "", 0)

    logger.info(f"获取{trading_day}的交易日价格统计数据")
    ticker_data = rest_api.query_trading_day_ticker(symbol, ticker_type)

    if ticker_data.empty:
        logger.warning(f"{trading_day}没有获取到交易日价格统计数据")
        return

    ticker_data["TradingDay"] = trading_day
    # 保存到CSV文件
    file_path = os.path.join(data_dir, f"{trading_day}_trading_day_ticker.csv")
    logger.info(f"{trading_day}的交易日价格统计数据大小: {ticker_data.shape}")
    ticker_data.to_csv(file_path, index=False)


if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")
    global_config.load_config(config_path)

    store_path = global_config.get(KEY_DATA_STORE)
    if not store_path:
        logger.error("数据存储路径未配置")
        exit(1)

    symbol = 'BTCUSDT'  # 交易对
    start_date = "2025-03-15"
    end_date = "2025-03-18"

    intervals = [Interval.MINUTE]
    for interval in intervals:
        fetch_all_klines(start_date, end_date, symbol, interval, store_path)

    # fetch_agg_traders(start_date, end_date, symbol)
    # fetch_historical_trades(start_date, end_date, symbol)
    # fetch_trading_day_ticker(start_date, symbol)
