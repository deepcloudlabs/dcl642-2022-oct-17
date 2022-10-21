import asyncio
import json

import pika
import websockets

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare('tradex2', durable=True, exchange_type='direct')
channel.queue_declare(queue='tradeq2')
channel.queue_bind(exchange='tradex2', queue='tradeq2', routing_key='')

async def consumer_handler(frames):
    async for frame in frames:
        trade = json.loads(frame)
        print(trade)
        channel.basic_publish(exchange='tradex2', routing_key='', body=trade)


async def connect_to_binance():
    async with websockets.connect("wss://stream.binance.com:9443/ws/btcusdt@trade") as ws:
        await consumer_handler(ws)


asyncio.run(connect_to_binance())
