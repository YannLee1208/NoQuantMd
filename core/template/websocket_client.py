"""
coding=utf-8
@File   : websocket_client.py
@Author : LiHan
@Time   : 3/12/25:1:49 PM
"""
import json
import traceback
from typing import Optional
import websocket
from threading import Thread
import ssl

from loguru import logger


class WebsocketClient:
    """
    Websocket客户端基类。
    重写以下函数:
    * on_open: websocket连接建立时调用。
    * on_close: websocket连接关闭时调用。
    * on_error: websocket连接发生错误时调用。
    * on_message: 接收到消息时调用。
    """

    def __init__(self):
        self.host: str = ""
        self.active: bool = False
        self.websocket_app: websocket.WebSocketApp = None
        self.thread: Thread = None

        self.proxy_host: Optional[str] = None
        self.proxy_port: Optional[int] = None
        self.header: Optional[dict] = None
        self.ping_interval_second: int = 0
        self.receive_timeout_second: int = 0
        self.verbose: bool = False

    def init(self, host: str, proxy_host: str, proxy_port: int,
             ping_interval_second: int = 30, receive_timeout_second: int = 60,
             verbose: bool = False):
        self.host = host

        if proxy_host and proxy_port:
            self.proxy_host = proxy_host
            self.proxy_port = proxy_port

        self.ping_interval_second = ping_interval_second

        websocket.enableTrace(verbose)
        websocket.setdefaulttimeout(receive_timeout_second)

    def start(self):
        """
        通过threading.Thread启动websocket连接。
        """
        self.active = True
        self.thread = Thread(target=self._run)
        self.thread.start()

    def join(self):
        """
        阻塞当前线程，直到websocket连接关闭。
        """
        if not self.active:
            logger.warning("Websocket is not active. Join is ignored.")
            return

        self.thread.join()

    def stop(self):
        """
        关闭websocket连接。
        """
        if not self.active:
            logger.warning("Websocket is not active. Stop is ignored.")

        self.active = False
        self.websocket_app.close()

    def send(self, msg: dict):
        """
        发送消息。
        :param msg: 要发送的消息，默认使用json进行dump
        """
        if not self.active:
            logger.warning("Websocket is not active. Send is ignored.")
            return

        msg_str = json.dumps(msg)
        self.websocket_app.send(msg_str)

    def _run(self):
        """
        使用websocket连接服务器，并保持连接，直到stop方法被调用。
        通过threading.Thread启动。
        """

        def on_open(websocket_app: websocket.WebSocket):
            self.on_open()

        def on_close(websocket_app: websocket.WebSocket, status_code: int, msg: str):
            self.on_close(status_code, msg)

        def on_error(websocket_app: websocket.WebSocket, error):
            self.on_error(error)

        def on_message(websocket_app: websocket.WebSocket, message):
            self.on_message(message)

        logger.debug(
            f"Connecting to websocket server: {self.host}, "
            f"{self.proxy_host=}, {self.proxy_port=}, "
            f"{self.ping_interval_second=}, {self.receive_timeout_second=}, "
            f"{self.verbose=}"
        )

        self.websocket_app = websocket.WebSocketApp(
            url=self.host,
            header=self.header,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message
        )

        proxy_type: Optional[str] = None
        if self.proxy_host:
            proxy_type = "http"

        self.websocket_app.run_forever(
            sslopt={"cert_reqs": ssl.CERT_NONE},
            ping_interval=self.ping_interval_second,
            http_proxy_host=self.proxy_host,
            http_proxy_port=self.proxy_port,
            proxy_type=proxy_type,
            reconnect=1
        )

    def on_open(self):
        logger.info("Websocket connection is opened.")

    def on_close(self, status_code: int, msg: str):
        logger.info(f"Websocket connection is closed")
        logger.info(f"{status_code=}, {msg=}")

    def on_error(self, error: Exception):
        try:
            logger.error(f"Websocket error occurred")
            logger.error(f"{error=}")
        except Exception:
            traceback.print_exc()

    def on_message(self, message: str):
        logger.debug(f"Received message: {message}")
        json_format_msg = json.loads(message)
        self.on_message(json_format_msg)
