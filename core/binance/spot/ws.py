"""
coding=utf-8
@File   : ws
@Author : LiHan
@Time   : 3/12/25:2:35 PM
"""
import json
from datetime import datetime
from loguru import logger

from core.binance.spot.gateway import BinanceSpotGateway
from core.template.constant import Exchange
from core.template.object import TickData, SubscribeRequest
from core.template.websocket_client import WebsocketClient
from core.utils.constant import Env

WEBSOCKET_DATA_HOST: dict[Env, str] = {
    Env.PROD: "wss://data-stream.binance.vision:443/ws",
    Env.TEST: "wss://testnet.binance.vision/ws/"
}

WEBSOCKET_RECEIVE_TIMEOUT_SECOND = 24 * 60 * 60



class BinanceSpotDataWebsocketApi(WebsocketClient):

    def __init__(self, gateway: BinanceSpotGateway):
        super(BinanceSpotDataWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.req_id: int = 0

    def connect(self, env: Env, proxy_host: str, proxy_port: int):
        host = WEBSOCKET_DATA_HOST[env]
        self.init(
            host=host,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            receive_timeout_second=WEBSOCKET_RECEIVE_TIMEOUT_SECOND
        )
        self.start()

    def subscribe(self, req: SubscribeRequest):
        if req.symbol in self.ticks:
            return

        # if req.symbol not in symbol_contract_map:
        #     logger.error(f"{self.gateway_name} {req.symbol} contract not found.")
        #     return

        self.req_id += 1

        tick: TickData = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=Exchange.BINANCE,
            local_time=datetime.now().timestamp(),
            exchange_time=datetime.now().timestamp(),
            gateway_name=gateway_name
        )

        tick.extra = {}
        self.ticks[req.symbol] = tick

        channels = [
            f"{req.symbol}@ticker",
            f"{req.symbol}@depth10",
            f"{req.symbol}@kline_1m"
        ]

        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.req_id
        }
        self.send(req)

    def on_open(self):
        """
        Callback when connection is opened.
        """
        logger.info(f"{self.gateway_name} websocket connection established.")

        # 自动重新订阅之前订阅的tick
        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@depth10")
                channels.append(f"{symbol}@kline_1m")

            req: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.req_id
            }
            self.send(req)

    def on_message(self, message: str):
        """
        Callback when message is received.
        :param message: str，默认使用json格式的字符串
        """
        data = json.loads(message)
        stream: str | None = data.get("stream", None)

        if not stream:
            logger.debug(f"{self.gateway_name} websocket message: {data}, no stream found.")
            return

        data: dict = data["data"]
        logger.debug(f"{self.gateway_name} websocket message: {data}")


if __name__ == '__main__':
    gateway_name = "binance_spot"
    ws_client = BinanceSpotDataWebsocketApi(BinanceSpotGateway(gateway_name))

    sub_req = SubscribeRequest(symbol="btcusdt", exchange=Exchange.BINANCE)
    ws_client.subscribe(sub_req)
    ws_client.connect(Env.PROD, "", 0)

    ws_client.join()
