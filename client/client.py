from common.config_setup import setup
import pika
import time
import logging
import csv
from common.dict_encoder_decoder import DictEncoderDecoder

config_params = setup('config.ini', {'INPUT_MATCHES_QUEUE': False, 'RABBIT_IP': False})
rabbit_ip = config_params['RABBIT_IP']
matches_queue = config_params['INPUT_MATCHES_QUEUE']

connection = pika.BlockingConnection(
pika.ConnectionParameters(host=rabbit_ip))
channel = connection.channel()

channel.queue_declare(queue=matches_queue)

with open('/sources/matches.csv', newline='') as csvf:
    reader = csv.DictReader(csvf)
    for row_dict in reader:
        serialized = DictEncoderDecoder.encode_dict(row_dict)
        channel.basic_publish(exchange='', routing_key=matches_queue, body=serialized)
        logging.info(f" [x] Sent line {serialized}")
        time.sleep(1)

connection.close()
