#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pika
import time
from jobs import read_blob_storage

from dotenv import load_dotenv
load_dotenv()

RMQ_USER=os.environ.get('RMQ_USER')
RMQ_PWD=os.environ.get('RMQ_PWD')
RMQ_SERVER_IP=os.environ.get('RMQ_SERVER_IP')
RMQ_VHOST=os.environ.get('RMQ_VHOST')

credentials = pika.PlainCredentials(RMQ_USER, RMQ_PWD)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RMQ_SERVER_IP, port='5672', virtual_host=RMQ_VHOST, credentials=credentials))

channel = connection.channel()

channel.queue_declare(queue='myjobqueue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode("utf-8"))
    # time.sleep(30)
    read_blob_storage.apply_async(args=[body.decode("utf-8")])
    # time.sleep(1)
    print(" [x] Done")
    # TODO: Uncomment line below to send delivery acknowlegment to the broker and to dequeue message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='myjobqueue', on_message_callback=callback)

channel.start_consuming()