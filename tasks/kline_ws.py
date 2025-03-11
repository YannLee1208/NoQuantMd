import asyncio
import websockets
import json


async def connect_to_binance():
    ws_url = "wss://data-stream.binance.vision:443/ws/btcusdt@kline_1m"

    async with websockets.connect(ws_url) as websocket:
        print(f"Connected to: {ws_url}")

        # 接收消息并打印
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            print(json.dumps(data, indent=4))  # 美化打印JSON内容


# 运行主协程
asyncio.get_event_loop().run_until_complete(connect_to_binance())