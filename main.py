from datetime import datetime, timedelta

from tasks.kline import fetch_all_klines

if __name__ == "__main__":
    symbol = 'BTCUSDT'  # 交易对
    interval = '1h'     # 时间间隔
    end_time = datetime.now()
    start_time = end_time - timedelta(days=365)

    klines = fetch_all_klines(symbol, interval, start_time, end_time)


    # 保存为CSV文件
    print(f'已保存{symbol}交易对过去一年的{interval}K线数据到CSV文件。')
