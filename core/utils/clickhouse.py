"""
coding=utf-8
@File   : clickhouse
@Author : LiHan
@Time   : 11/6/24:2:00 PM
"""
import time
from multiprocessing import get_context
from typing import Union

import clickhouse_connect
import pandas as pd
import polars as pl
from loguru import logger

import numpy as np


class ClickhouseClient:

    def __init__(
            self,
            host: str,
            username: str,
            passwd: str,
            port: int = 8123,
            database: str = "default",
    ):
        self.host = host
        self.username = username
        self.passwd = passwd
        self.port = port
        self.database = database
        self.compression = "lz4"

    def set_compress(self, compression: str):
        # local link table 不支持lz4压缩，需要用gzip
        self.compression = compression

    def get_client(self):
        client = clickhouse_connect.get_client(
            host=self.host,
            username=self.username,
            password=self.passwd,
            port=self.port,
            compression=self.compression
        )
        client.command("set connect_timeout_with_failover_ms = 1000;")
        client.command("set max_network_bandwidth = 3000000;")
        return client

    def command(self, sql):
        with self.get_client() as client:
            client.command(sql)

    def query(self, sql):
        with self.get_client() as client:
            return client.query(sql)

    def delete(self, table_name: str, condition: str):
        sql = f"ALTER TABLE {table_name} DELETE WHERE {condition}"
        self.command(sql)

    def query_dataframe(self, sql):
        result = self.query(sql)
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        return df

    def query_value(self, sql):
        result = self.query(sql)
        if len(result.result_rows) == 0:
            return None
        return result.result_rows[0][0]

    def insert_dataframe(self, table_name: str, df: Union[pd.DataFrame, pl.DataFrame], batch_size: int = 100000):
        """
        使用clickhouse_connect的原生insert_df方法批量插入数据
        """
        start = time.time()

        df = self._to_pandas(df)

        # 处理DataFrame中的特殊值，避免插入错误
        df = self._handle_special_values(df)

        total_rows = len(df)

        with self.get_client() as client:
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                # logger.info(f"插入数据到{table_name} {i} - {min(i + batch_size, total_rows)} / 总共 {total_rows}")
                client.insert_df(table_name, batch_df)

        end = time.time()
        duration = end - start
        # logger.info(
        #     f"插入数据到{table_name}成功, batch: {batch_size}, 耗时: {duration:.2f}秒, "
        #     f"行数: {total_rows}, 平均每秒插入: {total_rows / duration:.2f}行"
        # )

    def insert_dataframe_parallel(self, table_name: str, df: pd.DataFrame, batch_size: int = 100000,
                                  max_workers: int = 4):
        """
        并行批量插入数据
        """
        start = time.time()
        logger.info(f"开始并行插入数据到{table_name}")

        df = self._to_pandas(df)

        # 处理DataFrame中的特殊值
        df = self._handle_special_values(df)

        # 将数据分割成多个批次
        total_rows = len(df)
        batches = [df.iloc[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        # 准备连接参数
        connection_params = {
            'host': self.host,
            'username': self.username,
            'password': self.passwd,
            'database': self.database
        }

        # 准备进程池的参数
        batch_args = [
            (batch, connection_params, table_name, i, len(batches))
            for i, batch in enumerate(batches)
        ]

        # 使用进程池并行执行插入
        ctx = get_context("spawn")
        with ctx.Pool(processes=max_workers) as pool:
            results = pool.map(_insert_batch, batch_args)

        # 统计成功和失败的批次
        success_count = sum(results)
        failed_count = len(results) - success_count

        end = time.time()
        duration = end - start

        # 记录执行统计信息
        logger.info(
            f"多进程并行插入数据到{table_name}完成:\n"
            f"- 总批次数: {len(batches)}\n"
            f"- 成功批次: {success_count}\n"
            f"- 失败批次: {failed_count}\n"
            f"- 进程数: {max_workers}\n"
            f"- 批次大小: {batch_size}\n"
            f"- 总行数: {total_rows}\n"
            f"- 总耗时: {duration:.2f}秒\n"
            f"- 平均每秒插入: {total_rows / duration:.2f}行"
        )

    @staticmethod
    def _handle_special_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        处理DataFrame中的特殊值，避免插入错误
        """
        if "TradingDay" in df.columns:
            if df["TradingDay"].dtype == "object":
                df["TradingDay"] = pd.to_datetime(df["TradingDay"]).dt.date

        return df.replace([np.inf, -np.inf], np.nan)

    @staticmethod
    def _to_pandas(df: Union[pd.DataFrame, pl.DataFrame]) -> pd.DataFrame:
        """
        将 DataFrame 转换为 pandas.DataFrame，如果已经是 pandas.DataFrame 则无需转换
        """
        if isinstance(df, pd.DataFrame):
            return df
        elif isinstance(df, pl.DataFrame):
            return df.to_pandas()
        else:
            raise TypeError("DataFrame 类型必须为 pandas.DataFrame 或 polars.DataFrame")


def _insert_batch(args):
    """
    执行单个批次数据插入的独立函数
    必须定义在类外部以支持多进程序列化
    """
    batch_df, connection_params, table_name, batch_num, total_batches = args
    try:
        client = clickhouse_connect.get_client(**connection_params)
        client.insert_df(table_name, batch_df)
        client.close()
        return True
    except Exception as e:
        logger.error(f"批次 {batch_num + 1}/{total_batches} 插入失败: {str(e)}")
        return False


def sync_data_from_remote(
        from_client: ClickhouseClient,
        to_client: ClickhouseClient,
        from_table_name: str,
        trading_day: str,
        to_table_name: str = None,
):
    # 1. 删除目标表数据
    if to_table_name is None:
        to_table_name = from_table_name
    logger.info(f"删除目标表{to_table_name}数据")
    to_client.delete(to_table_name, f"TradingDay = '{trading_day}'")

    # 2. 插入数据
    logger.info(f"从{from_client.host} 同步数据{from_table_name} 到{to_client.host} {to_table_name}")
    sql = f"""
           INSERT INTO {to_table_name}
           SELECT *
           FROM remote('{from_client.host}', '{from_table_name}', '{from_client.username}', '{from_client.passwd}')
           WHERE TradingDay='{trading_day}' 
    """
    try:
        to_client.command(sql)
    except Exception as _:
        import traceback
        exec = traceback.format_exc()
        logger.error(f"同步数据失败, {exec}")
        exit(1)
    logger.info(f"同步数据{from_table_name}完成")