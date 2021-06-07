from common.config_setup import setup
import pika
import time
import logging
import csv
from common.match_encoder_decoder import MatchEncoderDecoder
from common.batch_encoder_decoder import BatchEncoderDecoder

config_params = setup('config.ini', {'INPUT_MATCHES_QUEUE': False, 'RABBIT_IP': False})
rabbit_ip = config_params['RABBIT_IP']
matches_queue = config_params['INPUT_MATCHES_QUEUE']

connection = pika.BlockingConnection(
pika.ConnectionParameters(host=rabbit_ip))
channel = connection.channel()

channel.queue_declare(queue=matches_queue)

BATCH_SIZE = 200

with open('/sources/matches.csv', newline='') as csvf:
    reader = csv.DictReader(csvf)
    batch = []
    count = 0
    for row_dict in reader:
        match_dict = MatchEncoderDecoder.parse_dict(row_dict)
        batch.append(match_dict)
        count += 1

        if count >= BATCH_SIZE:
            serialized = BatchEncoderDecoder.encode_batch(batch)
            channel.basic_publish(exchange='', routing_key=matches_queue, body=serialized)
            logging.info(f" [x] Sent batch {serialized[:25]}")
            batch = []
            count = 0
            # time.sleep(5)

    if count > 0:
        serialized = BatchEncoderDecoder.encode_batch(batch)
        channel.basic_publish(exchange='', routing_key=matches_queue, body=serialized)
        logging.info(f" [x] Sent last missing batch {serialized[:25]}")
    
connection.close()
