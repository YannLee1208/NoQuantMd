"""
coding=utf-8
@File   : shm
@Author : LiHan
@Time   : 3/14/25:11:28 AM
"""

from multiprocessing import shared_memory
import numpy as np


def init_md_shm(symbol: str) -> tuple[shared_memory.SharedMemory, np.ndarray]:
    """
    初始化合并数据的共享内存块
    :param symbol: 交易symbol
    :return: 共享内存块和结构化数组
    """
    # 计算内存尺寸（网页5的精确计算）
    dtype = np.dtype([
        ('last_price', 'f8'),
        ('volume_24h', 'f8'),
        ('high_price', 'f8'),
        ('low_price', 'f8'),
        ('bid_prices', 'f8', 10),
        ('bid_volumes', 'f8', 10),
        ('ask_prices', 'f8', 10),
        ('ask_volumes', 'f8', 10),
        ('update_time', 'i8')
    ])

    shm = shared_memory.SharedMemory(
        name=f"binance_{symbol}_merged",
        create=True,
        size=dtype.itemsize
    )

    # 映射为结构化数组（网页6的高效访问）
    arr = np.ndarray(shape=(), dtype=dtype, buffer=shm.buf)
    return shm, arr
