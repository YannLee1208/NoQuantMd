import os
from datetime import datetime, timezone
from loguru import logger

from core.binance.spot.rest import BinanceSpotDataRestAPi
from core.constant.data import DATA_BASE_DIR
from core.constant.object import Interval, Exchange
from core.utils.constant import Env
from core.utils.date import cal_date_interval


def fetch_all_klines(start_trading_day: str, end_trading_day: str,
                     symbol: str, interval: Interval):
    """
    获取指定时间范围内的所有K线数据
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :return: None
    """
    data_dir = os.path.join(DATA_BASE_DIR, Exchange.BINANCE.value, "spot", symbol, interval.value)
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


if __name__ == '__main__':
    symbol = 'BTCUSDT'  # 交易对
    start_date = "2024-01-01"
    end_date = "2024-12-31"

    intervals = [Interval.MINUTE]
    for interval in intervals:
        fetch_all_klines(start_date, end_date, symbol, interval)
