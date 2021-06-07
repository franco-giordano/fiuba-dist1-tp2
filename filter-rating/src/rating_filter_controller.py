import pika
from common.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_rating_player import FilterRatingPlayer
import logging

class RatingFilterController:
    def __init__(self, rabbit_ip, players_exchange_name, output_exchange_name, route_q2, route_q4):
        self.players_exchange_name = players_exchange_name
        self.output_exchange_name = output_exchange_name
        self.route_q2 = route_q2
        self.route_q4 = route_q4

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))
        self.channel = self.connection.channel()

        # setup input exchange
        self.channel.exchange_declare(exchange=self.players_exchange_name, exchange_type='fanout')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.players_exchange_name, queue=queue_name)
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self._callback, auto_ack=True)

        # setup output exchange
        self.channel.exchange_declare(exchange=self.output_exchange_name, exchange_type='direct')

        self.filter = FilterRatingPlayer()

    def run(self):
        logging.info('RATING FILTER: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"RATING FILTER: Received batch {body[:25]}...")

        passing_q2 = []
        passing_q4 = []

        for player in batch:
            pass_q2, pass_q4 = self.filter.should_pass(player)
            if pass_q2:
                passing_q2.append(player)
            if pass_q4:
                passing_q4.append(player)

        if passing_q2:
            logging.info(f'RATING FILTER: Announcing passing players for query 2...')
            serialized = BatchEncoderDecoder.encode_batch(passing_q2)
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=self.route_q2, body=serialized)
        if passing_q4:
            logging.info(f'RATING FILTER: Announcing passing players for query 4...')
            serialized = BatchEncoderDecoder.encode_batch(passing_q4)
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=self.route_q4, body=serialized)
