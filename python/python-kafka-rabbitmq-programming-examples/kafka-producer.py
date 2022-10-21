import asyncio
import json

import websockets
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda m: json.dumps(m).encode('utf-8'))

"""
{'e': 'trade', 'E': 1665749275108, 's': 'BTCUSDT', 't': 1969562438, 'p': '19635.47000000', 'q': '0.00141000', 'b': 14389316511, 'a': 14389316531, 'T': 1665749275108, 'm': True, 'M': True}
"""
async def consumer_handler(frames):
    async for frame in frames:
        trade = json.loads(frame)
        print(trade)
        producer.send("trades", value=frame)


async def connect_to_binance():
    async with websockets.connect("wss://stream.binance.com:9443/ws/btcusdt@trade") as ws:
        await consumer_handler(ws)


asyncio.run(connect_to_binance())
