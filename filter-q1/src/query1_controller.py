import pika
from common.match_encoder_decoder import MatchEncoderDecoder
from common.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query1 import FilterQuery1
import logging

class Query1Controller:
    def __init__(self, rabbit_ip, input_queue_name, output_queue_name):
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.input_queue_name)
        self.channel.queue_declare(queue=self.output_queue_name)

        self.channel.basic_consume(
            queue=self.input_queue_name, on_message_callback=self._callback, auto_ack=True)

        self.filter = FilterQuery1()


    def run(self):
        logging.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f" [x] Received batch and parsed as {batch[:25]}")

        passing = list(filter(self.filter.should_pass, batch))

        if passing:
            logging.info(f' [Y] Sending to output queue the passing matches {passing}')
            serialized = BatchEncoderDecoder.encode_batch(passing)
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)
