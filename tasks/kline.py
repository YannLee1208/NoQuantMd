import os
from datetime import datetime, timezone

from loguru import logger

from api.kline import get_klines
from constant.clickhouse import DATABASE_KLINE, INTERVAL_TABLE_MAPPING
from constant.data import DATA_BASE_DIR
from utils.clickhouse import ClickhouseClient
from utils.date import cal_date_interval



def fetch_all_klines(client: ClickhouseClient, table_name: str, start_trading_day: str, end_trading_day: str, symbol: str, interval: str):
    """
    获取指定时间范围内的所有K线数据
    :param client: Clickhouse客户端
    :param table_name: 表名
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol: 交易Symbol，如BTCUSDT
    :param interval: k线周期
    :return: None
    """
    data_dir = os.path.join(DATA_BASE_DIR, symbol, interval)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    days = cal_date_interval(start_trading_day, end_trading_day)
    for day in days:
        logger.info(f"获取{day}的K线数据")
        start_timestamp = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_timestamp = start_timestamp + 24 * 60 * 60 * 1000 - 1
        klines = get_klines(symbol, interval, start_timestamp, end_timestamp)
        if klines.empty:
            logger.warning(f"{day}没有获取到K线数据")
            break
        klines["TradingDay"] = day
        # 保存到CSV文件
        file_path = os.path.join(data_dir, f"{day}.csv")
        klines.to_csv(file_path, index=False)
        logger.info(f"{day}的K线数据大小: {klines.shape}")
        client.delete(table_name, f"TradingDay = '{day}'")
        client.insert_dataframe(table_name, klines)

if __name__ == '__main__':
    symbol = 'BTCUSDT'  # 交易对
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    client = ClickhouseClient(host="localhost", port=8123, username="", passwd="")

    intervals = ['1s']
    for interval in intervals:
        table_name = INTERVAL_TABLE_MAPPING[interval]
        table_name = f"{DATABASE_KLINE}.{table_name}"
        fetch_all_klines(client, table_name, start_date, end_date, symbol, interval)
