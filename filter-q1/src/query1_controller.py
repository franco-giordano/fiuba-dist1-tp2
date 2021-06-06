import pika
from common.dict_encoder_decoder import DictEncoderDecoder
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
        logging.info(f" [x] Received {body}. Will deserialize...")
        match_dict = DictEncoderDecoder.decode_bytes(body)
        logging.info(f" [x] Parsed as {match_dict}")

        should_pass = self.filter.should_pass(match_dict)

        if should_pass:
            logging.info(f' [Y] Match {match_dict} should pass! Sending to output queue')
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=DictEncoderDecoder.encode_dict(match_dict))
