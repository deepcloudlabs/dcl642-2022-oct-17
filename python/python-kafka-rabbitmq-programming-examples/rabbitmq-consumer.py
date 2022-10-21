import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port='7100'))
channel = connection.channel()
channel.exchange_declare('test', durable=True, exchange_type='topic')


def callbackFunctionForQueue(ch, method, properties, body):
    print('Got a message from queue: ', body)


channel.basic_consume(queue='tradeq2', on_message_callback=callbackFunctionForQueue, auto_ack=True)
channel.start_consuming()
