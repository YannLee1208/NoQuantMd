"""
coding=utf-8
@File   : ws
@Author : LiHan
@Time   : 3/12/25:2:35 PM
"""
import json
from datetime import datetime
from loguru import logger

from external.object import Exchange, Interval
from external.object import TickData, SubscribeRequest, KLineData
from external.websocket_client import WebsocketClient
from core.utils.constant import Env

WEBSOCKET_DATA_HOST: dict[Env, str] = {
    # Env.PROD: "wss://data-stream.binance.vision:443/ws",
    Env.PROD: "wss://stream.binance.com:9443/stream",
    Env.TEST: "wss://testnet.binance.vision/ws/"
}

WEBSOCKET_RECEIVE_TIMEOUT_SECOND = 24 * 60 * 60

CHANNELS = ["ticker", "depth10", "kline_1m"]
# CHANNELS = ["ticker", "depth10"]
KLINE_INTERVAL = Interval.MINUTE  # 和上面的CHANNELS对应


class BinanceSpotDataWebsocketApi(WebsocketClient):

    def __init__(self):
        super(BinanceSpotDataWebsocketApi, self).__init__()

        self.gateway_name = "binance_spot_data_ws"

        self.ticks: dict[str, TickData] = {}
        self.req_id: int = 0
        self.pending_subscriptions: list[SubscribeRequest] = []

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

        self.req_id += 1

        tick: TickData = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=Exchange.BINANCE,
            local_time=datetime.now().timestamp(),
            exchange_time=datetime.now().timestamp(),
            gateway_name=self.gateway_name
        )

        tick.extra = {}
        self.ticks[req.symbol] = tick

        # 仅在连接活跃时发送订阅，否则将会在连接后自动订阅
        if not self.active or not self.websocket_app:
            self.pending_subscriptions.append(req)
            return

        # 发送订阅请求
        self._send_subscription(req)

    def _send_subscription(self, req: SubscribeRequest):

        channels = [f"{req.symbol}@{channel}" for channel in CHANNELS]

        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.req_id
        }
        self.send(req)

    def on_open(self):
        """
        当websocket连接建立时调用
        """
        logger.info(f"{self.gateway_name} websocket connection established.")

        if self.pending_subscriptions:
            for req in self.pending_subscriptions:
                self._send_subscription(req)
            self.pending_subscriptions.clear()

    def on_message(self, message: str):
        """
        当websocket收到消息时调用
        :param message: str，默认使用json格式的字符串
        """
        data = json.loads(message)
        logger.debug(f"{self.gateway_name} data received: {data}")

        stream: str = data.get("stream", None)

        if not stream:
            logger.error(f"{self.gateway_name} stream not found in data: {data}")
            return

        symbol, channel = stream.split('@')
        data = data.get("data", None)

        tick: TickData = self.ticks.get(symbol, None)
        if channel == "ticker":
            if tick:
                tick.volume = float(data['v'])
                tick.turnover = float(data['q'])
                tick.open_price = float(data['o'])
                tick.high_price = float(data['h'])
                tick.low_price = float(data['l'])
                tick.last_price = float(data['c'])
                tick.exchange_time = data['E']
                tick.local_time = datetime.now().timestamp()
        elif channel == "depth10":
            bids = data['bids']
            for n in range(min(10, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["asks"]
            for n in range(min(10, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))
        else:
            if data['e'] == "kline":
                bar_ready: bool = data.get('x', False)  # 是否是完整的k线数据
                if not bar_ready:
                    logger.debug(f"{self.gateway_name} {symbol} kline is not ready.")
                    return

                kline_data = data['k']
                tick.extra["kline"] = KLineData(
                    symbol=symbol.upper(),
                    exchange=Exchange.BINANCE,
                    exchange_time=data['E'],
                    local_time=datetime.now().timestamp(),
                    interval=KLINE_INTERVAL,
                    volume=float(kline_data['v']),
                    turnover=float(kline_data['q']),
                    open_price=float(kline_data['o']),
                    high_price=float(kline_data['h']),
                    low_price=float(kline_data['l']),
                    close_price=float(kline_data['c']),
                    gateway_name=self.gateway_name
                )
            else:
                logger.error(f"{self.gateway_name} unknown data received: {data}")
                return


if __name__ == '__main__':
    ws_client = BinanceSpotDataWebsocketApi()

    symbols = ["btcusdt", "ethusdt"]

    ws_client.connect(Env.PROD, "", 0)

    for symbol in symbols:
        sub_req = SubscribeRequest(symbol=symbol, exchange=Exchange.BINANCE)
        ws_client.subscribe(sub_req)
    ws_client.join()
