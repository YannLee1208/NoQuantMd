from datetime import datetime, timedelta


def cal_date_interval(start_date: str, end_date: str) -> list[str]:
    """
    计算日期区间内的所有日期
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    while start <= end:
        date_list.append(start.strftime("%Y-%m-%d"))
        start += timedelta(days=1)
    return date_list