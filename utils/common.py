import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_requests_session():
    session = requests.Session()
    retry = Retry(
        total=5,  # 重试总次数
        backoff_factor=1,  # 退避因子，重试之间等待的时间为 {backoff factor} * (2 ** (重试次数 - 1))
        status_forcelist=[429, 500, 502, 503, 504],  # 针对这些状态码进行重试
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # 针对这些方法重试（旧版本 urllib3 使用 method_whitelist）
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
