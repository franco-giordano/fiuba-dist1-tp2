import pika
from common.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query1 import FilterQuery1
import logging

class Query1Controller:
    def __init__(self, rabbit_ip, matches_exchange_name, output_queue_name):
        self.matches_exchange_name = matches_exchange_name
        self.output_queue_name = output_queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.matches_exchange_name, exchange_type='fanout')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.matches_exchange_name, queue=queue_name)
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self._callback, auto_ack=True)

        self.channel.queue_declare(queue=self.output_queue_name)
        self.filter = FilterQuery1()

    def run(self):
        logging.info('FILTER QUERY1: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"FILTER QUERY1: Received batch {body[:25]}...")

        passing = list(filter(self.filter.should_pass, batch))

        if passing:
            logging.info(f'FILTER QUERY1: Sending to output queue the passing matches {passing}')
            serialized = BatchEncoderDecoder.encode_batch(passing)
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)
