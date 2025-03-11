from utils.clickhouse import ClickhouseClient
import pandas as pd
if __name__ == '__main__':
    table = "kline.one_second_binance"
    client = ClickhouseClient(host="localhost", port=8123, username="", passwd="")
    data = pd.read_csv(f"../raw_data/ck_backup_data/{table}.csv")
    data[
        "OpenTime"
    ] = pd.to_datetime(data["OpenTime"])
    data[
        "CloseTime"
    ] = pd.to_datetime(data["CloseTime"])
    print(data.shape)

    client.insert_dataframe_parallel(df=data, table_name=table)