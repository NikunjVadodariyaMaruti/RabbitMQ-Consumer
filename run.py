import json
import os
from enum import Enum

import pika
import requests

ES_HOST = None
INDEX_NAME = None

RABBITMQ_USER = None
RABBITMQ_PASSWRD = None
RABBITMQ_HOST = None

thread_id = 0
processed_thread_id_list = []

HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ZWxhc3RpYzo1KXEuV0NQQw==',
}


class QueueEnum(Enum):
    CONVERSATION_DATA_RECOVERY = {'route': 'conversation_summary_data_recovery',
                                  'queue': 'conversation_summary_data_recovery_q',
                                  'exchange_type': 'direct', 'exchange_name': 'wotnot.direct'}
    FAILED_CONVERSATION_DATA_RECOVERY = {'route': 'failed_conversation_summary_data_recovery',
                                         'queue': 'failed_conversation_summary_data_recovery_q',
                                         'exchange_type': 'direct', 'exchange_name': 'wotnot.direct'}


def queue_connect():
    parameters = pika.URLParameters(RABBIT_MQ_URL_PARAMETER)
    connection = pika.BlockingConnection(parameters)
    return connection


def queue_close(connection):
    if connection is not None:
        connection.close()


class RabbitMqConnection(object):
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = queue_connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        queue_close(self.conn)


def create_queues():
    with RabbitMqConnection() as conn:
        queue = QueueEnum.CONVERSATION_DATA_RECOVERY.value
        channel = conn.channel()
        channel.exchange_declare(exchange=queue['exchange_name'], exchange_type=queue['exchange_type'], durable=True)
        channel.queue_declare(queue=queue['queue'], durable=True)
        channel.queue_bind(exchange=queue['exchange_name'], routing_key=queue['route'], queue=queue['queue'])

        queue = QueueEnum.FAILED_CONVERSATION_DATA_RECOVERY.value
        channel = conn.channel()
        channel.exchange_declare(exchange=queue['exchange_name'], exchange_type=queue['exchange_type'], durable=True)
        channel.queue_declare(queue=queue['queue'], durable=True)
        channel.queue_bind(exchange=queue['exchange_name'], routing_key=queue['route'], queue=queue['queue'])


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


def get_global_channel_name_by_channel_id(channel_id):
    return {
        "1": "Web",
        "2": "Web",
        "3": "Messenger",
        "4": "SMS",
        "5": "WhatsApp",
        "6": "WhatsApp"
    }.get(channel_id)


def elastic_search_get_payload(data):
    location_data = json.loads(data['location_data']) if data['location_data'] else {}
    return json.dumps(
        {
            "doc":
                {
                    'bot_id': data['bot_id'],
                    'browser_language': data['browser_language'],
                    'channel_id': data['channel_id'],
                    'conversation_created_at': data['conversation_created_at'],
                    'conversation_title': data['conversation_title'],
                    'event_type_id': data['event_type_id'],
                    'ip_address': data['ip_address'],
                    'assigned_to': data['assigned_to'],
                    'last_activity_at': data['last_activity_at'],
                    'last_message_at': data['last_activity_at'],
                    'last_activity_by': data['last_activity_by'],
                    'message': data['message'],
                    'mode_id': data['mode_id'],
                    'note': data['note'],
                    'source_url': data['source_url'],
                    'status_id': data['status_id'],
                    'thread_id': data['thread_id'],
                    'thread_key': data['thread_key'],
                    'visitor_id': data['unique_user_id'],
                    'timezone': data['timezone'],
                    'global_channel_name': get_global_channel_name_by_channel_id(str(data['channel_id'])),
                    'visitor_key': data['visitor_key'],
                    'total_messages_count': data['total_messages_count'],
                    'unanswered_messages_count': data['unanswered_messages_count'],
                    'city_name': location_data.get('cityName', ''),
                    'country_code': location_data.get('countryCode'),
                    'country_name': location_data.get('countryName'),
                    'zip_code': location_data.get('zipCode'),
                    'visitor_name': '',
                    'visitor_messages': data['visitor_messages']
                },
            "doc_as_upsert": True
        })


def elastic_search_get_metadata(doc_id):
    return json.dumps({"update": {"_index": INDEX_NAME, "_id": doc_id}})


def elastic_search_insert(payload):
    payload_string = ""
    for data in payload:
        payload_string += elastic_search_get_metadata(
            data['thread_id']) + "\n" + elastic_search_get_payload(data) + "\n"
    _url = "{es_host}/_bulk".format(es_host=ES_HOST)
    response = requests.request("POST", _url, headers=HEADERS, data=payload_string)
    print(response.__dict__)


if __name__ == '__main__':
    ES_HOST = input("Enter Elastic search: ") or os.environ.get("ELASTICSEARCH_SERVICE_URL")
    INDEX_NAME = input("Enter Elastic search index: ") or os.environ.get("ES_INDEX_CONVERSATION_SUMMARY")
    RABBITMQ_USER = input("RABBIT_MQ_USERNAME: ") or os.environ.get("RABBIT_MQ_USERNAME")
    RABBITMQ_PASSWRD = input("RABBIT_MQ_PASSWORD: ") or os.environ.get("RABBIT_MQ_PASSWORD")
    RABBITMQ_HOST = input("RABBIT_MQ_HOST: ") or os.environ.get("RABBIT_MQ_HOST")

    RABBIT_MQ_URL_PARAMETER = 'amqp://{}:{}@{}:5672/%2F?heartbeat_interval:0'.format(
        RABBITMQ_USER, RABBITMQ_PASSWRD, RABBITMQ_HOST)
    print("*** Process Start ***")

    connection = pika.BlockingConnection(pika.URLParameters(RABBIT_MQ_URL_PARAMETER))
    channel = connection.channel()

    channel.queue_declare(queue=QueueEnum.CONVERSATION_DATA_RECOVERY.value['queue'], durable=True)
    channel.queue_declare(queue=QueueEnum.FAILED_CONVERSATION_DATA_RECOVERY.value['queue'], durable=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')

    queue_init()


    def callback(ch, method, properties, body):
        params = body.decode('utf-8')
        print("Received Message {data}: ".format(data=json.loads(params)))
        try:
            elastic_search_insert(json.loads(params))
        except Exception as e:
            queue = QueueEnum.FAILED_CONVERSATION_DATA_RECOVERY.value
            publish_message(queue['route'], payload=json.loads(params), exchange_name=queue['exchange_name'],
                            exchange_type=queue['exchange_type'])
            msg = 'Error: {error}'.format(error=e)
            print(msg)
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=50)
    channel.basic_consume(on_message_callback=callback,
                          queue=QueueEnum.CONVERSATION_DATA_RECOVERY.value['queue'])

    channel.start_consuming()
