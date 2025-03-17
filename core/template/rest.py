"""
coding=utf-8
@File   : rest
@Author : LiHan
@Time   : 3/14/25:9:57 AM
"""
import sys
import traceback
from datetime import datetime
from multiprocessing.dummy import Pool
from queue import Queue, Empty

import requests
from loguru import logger
from typing import Callable, Type, Optional
from types import TracebackType
from enum import Enum

CALLBACK_TYPE = Callable[[dict, "Request"], object]
ON_FAILED_TYPE = Callable[[int, "Request"], object]
ON_ERROR_TYPE = Callable[[Type, Exception, TracebackType, "Request"], object]


class RequestStatus(Enum):
    READY = 0
    SUCCESS = 1
    FAILED = 2
    ERROR = 3


class RestRequest:

    def __init__(self, method: str, path: str, params: dict = None, data: dict = None, headers: dict = None,
                 callback: CALLBACK_TYPE = None, on_failed: ON_FAILED_TYPE = None, on_error: ON_ERROR_TYPE = None):
        """
        :param method: 请求方法，如：GET、POST
        :param path: 请求路径，如：/api/v1/ping
        :param params: 请求参数
        :param data: 请求数据
        :param headers: 请求头
        :param callback: 请求成功的回调函数
        :param on_failed: 请求失败的回调函数
        :param on_error: 请求错误的回调函数
        """
        self.method: str = method
        self.path: str = path
        self.params: dict = params
        self.data: dict = data
        self.headers: dict = headers
        self.callback: CALLBACK_TYPE = callback
        self.on_failed: ON_FAILED_TYPE = on_failed
        self.on_error: ON_ERROR_TYPE = on_error

        self.response: requests.Response = None
        self.status: RequestStatus = RequestStatus.READY

    def __str__(self) -> str:
        if self.response is None:
            status_code = "terminated"
        else:
            status_code = self.response.status_code

        return (
            "request : {} {} {} because {}: \n"
            "headers: {}\n"
            "params: {}\n"
            "data: {}\n"
            "response:"
            "{}\n".format(
                self.method,
                self.path,
                self.status.name,
                status_code,
                self.headers,
                self.params,
                self.data,
                "" if self.response is None else self.response.text,
            )
        )


class RestClient:

    def __init__(self):
        self.url_base: str = ""
        self.active: bool = False
        self.proxies: dict = {}  # 代理，key为http或https，value为代理地址

        self.pool: Pool = None  # 进程池
        self.queue: Queue = Queue()  # 请求队列

    def init(
            self,
            url_base: str,
            proxy_host: str = "",
            proxy_port: int = 0
    ):
        """
        设置RestClient的基本信息
        :param url_base: url_base，如：https://api.binance.com
        :param proxy_host: 代理主机
        :param proxy_port: 代理端口
        """
        self.url_base = url_base

        if proxy_host and proxy_port:
            proxy: str = f"http://{proxy_host}:{proxy_port}"
            self.proxies = {"http": proxy, "https": proxy}

    def start(self, session_count: int = 1):
        """
        启动RestClient
        :param session_count: session的数量
        """
        logger.info("启动RestClient")
        if self.active:
            logger.warning("RestClient已经启动")
            return

        self.active = True
        self.pool = Pool(session_count)
        self.pool.apply_async(self._start)

    def add_request(self, method: str, path: str, callback: CALLBACK_TYPE, params: dict = None, data: dict = None,
                    headers: dict = None, on_failed: ON_FAILED_TYPE = None,
                    on_error: ON_ERROR_TYPE = None):
        """
        添加请求，异步非阻塞
        :param method: 请求方法，如：GET、POST
        :param path: 请求路径，如：/api/v1/ping
        :param params: 请求参数
        :param data: 请求数据
        :param headers: 请求头
        :param callback: 请求成功的回调函数
        :param on_failed: 请求失败的回调函数
        :param on_error: 请求错误的回调函数
        """
        request: RestRequest = RestRequest(
            method=method,
            path=path,
            params=params,
            data=data,
            headers=headers,
            callback=callback,
            on_failed=on_failed,
            on_error=on_error
        )
        self.queue.put(request)

    def request(self, method: str, path: str, params: dict = None, data: dict = None,
                headers: dict = None) -> requests.Response:
        """
        发起请求，同步阻塞
        :param method: 请求方法，如：GET、POST
        :param path: 请求路径，如：/api/v1/ping
        :param params: 请求参数
        :param data: 请求数据
        :param headers: 请求头
        :return: requests.Response
        """
        url: str = self.url_base + path

        try:
            response: requests.Response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=data,
                proxies=self.proxies
            )

            return response
        except Exception as e:
            error_type, error_value, traceback = sys.exc_info()
            self.on_error(error_type, e, traceback, None)

    def _start(self):
        try:
            session: requests.Session = requests.Session()
            while self.active:
                try:
                    request: RestRequest = self.queue.get(timeout=1)
                    try:
                        self._process_request(session, request)
                    finally:
                        self.queue.task_done()
                except Empty:
                    pass
        except Exception as e:
            error_type, error_value, traceback = sys.exc_info()
            self.on_error(error_type, e, traceback, None)

    def _process_request(self, session: requests.Session, request: RestRequest):
        try:
            url: str = self.url_base + request.path
            response: requests.Response = session.request(
                method=request.method,
                url=url,
                headers=request.headers,
                params=request.params,
                data=request.data,
                proxies=self.proxies
            )
            response.raise_for_status()

            request.response = response
            self._process_response_status(response, request)

        except Exception:
            request.status = RequestStatus.ERROR
            error_type, error_value, traceback = sys.exc_info()
            if request.on_error:
                request.on_error(error_type, error_value, traceback, request)
            else:
                self.on_error(error_type, error_value, traceback, request)

    def _process_response_status(self, response: requests.Response, request: RestRequest):
        status_code: int = response.status_code
        if status_code // 100 == 2:  # 2xx表示成功
            logger.info("===> 请求成功, 状态码：{}".format(status_code))
            json_body: dict = {}
            if status_code != 204:
                json_body = response.json()

            request.callback(json_body, request)
            request.status = RequestStatus.SUCCESS
        else:
            request.status = RequestStatus.FAILED
            if request.on_failed:
                request.on_failed(status_code, request)
            else:
                self.on_failed(status_code, request)

    def on_failed(self, status_code: int, request: RestRequest):
        """
        处理请求失败，非200状态码，可以重写此方法
        如果没有设置on_failed，会调用此方法
        :param status_code: 状态码
        :param request: 请求
        """
        logger.error("===> on_failed")
        logger.error(f"请求失败：{request}")
        logger.error(f"状态码：{status_code}")

    def on_error(self, error_type: type, error_value: Exception, traceback_info: TracebackType,
                 request: Optional[RestRequest]):
        """
        处理请求错误，可以重写此方法
        如果没有设置on_error，会调用此方法
        :param error_type: 错误类型
        :param error_value: 错误值
        :param traceback_info: 错误堆栈
        :param request: 请求
        """
        try:
            logger.error("===> on_error")
            text: str = "[{}]: Unhandled RestClient Error:{}\n".format(
                datetime.now().isoformat(), error_type
            )
            text += "request:{}\n".format(request)
            text += "Exception trace: \n"
            text += "".join(
                traceback.format_exception(error_type, error_value, traceback_info)
            )
            logger.error(text)
        except Exception:
            traceback.print_exc()
