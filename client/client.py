from common.config_setup import setup
import pika
import time
import logging

config_params = setup('config.ini', {'MSG': ('msg', False)})
msg = config_params['msg']
connection = pika.BlockingConnection(
pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='hello')

with open('/sources/matches.csv', 'r') as f:
    for line in f:
        channel.basic_publish(exchange='', routing_key='hello', body=line)
        logging.info(f" [x] Sent line {line}")
        time.sleep(1)

connection.close()
