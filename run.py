import json
import os
from enum import Enum
import pika


class QueueEnum(Enum):
    TEST_QUEUE = {'route_key': 'test_queue', 'queue_name': 'test_queue_q', 'exchange_type': 'direct',
                  'exchange_name': 'wotnot.direct'}
    FAILED_TEST_QUEUE = {'route_key': 'failed_test_queue', 'queue_name': 'failed_test_queue_q',
                         'exchange_type': 'direct',
                         'exchange_name': 'wotnot.direct'}


def connect():
    parameters = pika.URLParameters(RABBIT_MQ_URL_PARAMETER)
    connection = pika.BlockingConnection(parameters)
    return connection


def close(connection):
    if connection is not None:
        connection.close()


class RabbitMqConnection(object):
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        close(self.conn)


def create_queues():
    with RabbitMqConnection() as conn:
        for data in QueueEnum:
            channel = conn.channel()
            channel.exchange_declare(exchange=data.value['exchange_name'],
                                     exchange_type=data.value['exchange_type'], durable=True)
            channel.queue_declare(queue=data.value['queue_name'], durable=True)
            channel.queue_bind(exchange=data.value['exchange_name'],
                               routing_key=data.value['route_key'],
                               queue=data.value['queue_name'])


def queue_init():
    create_queues()


def publish_message(route_key, payload, exchange_name, exchange_type, delivery_mode=2):
    with RabbitMqConnection() as conn:
        channel = conn.channel()
        channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        channel.basic_publish(exchange=exchange_name,
                              routing_key=route_key,
                              body=json.dumps(payload),
                              properties=pika.BasicProperties(
                                  delivery_mode=delivery_mode,
                              ))


def start_consumer():
    with RabbitMqConnection() as conn:
        channel = conn.channel()
        channel.basic_qos(prefetch_count=50)
        channel.basic_consume(on_message_callback=callback,
                              queue=QueueEnum.TEST_QUEUE.value['queue_name'])
        channel.start_consuming()


def callback(ch, method, properties, body):
    params = body.decode('utf-8')
    try:
        print("Received Message {data}: ".format(data=json.loads(params)))
    except Exception as e:
        queue = QueueEnum.FAILED_TEST_QUEUE.value
        publish_message(queue['route_key'], payload=json.loads(params), exchange_name=queue['exchange_name'],
                        exchange_type=queue['exchange_type'])
        msg = 'Error: {error}'.format(error=e)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    RABBITMQ_USER = input("RABBIT_MQ_USERNAME: ") or os.environ.get("RABBIT_MQ_USERNAME")
    RABBITMQ_PASSWORD = input("RABBIT_MQ_PASSWORD: ") or os.environ.get("RABBIT_MQ_PASSWORD")
    RABBITMQ_HOST = input("RABBIT_MQ_HOST: ") or os.environ.get("RABBIT_MQ_HOST")

    RABBIT_MQ_URL_PARAMETER = 'amqp://{}:{}@{}:5672/%2F?heartbeat_interval:0'.format(
        RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_HOST)

    queue_init()
    print("Consumer is being started")
    start_consumer()
