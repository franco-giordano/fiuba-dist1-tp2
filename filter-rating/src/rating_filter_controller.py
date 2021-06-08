from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_rating_player import FilterRatingPlayer
from common.utils.rabbit_utils import RabbitUtils
import logging

class RatingFilterController:
    def __init__(self, rabbit_ip, players_exchange_name, output_exchange_name, route_q2, route_q4):
        self.players_exchange_name = players_exchange_name
        self.output_exchange_name = output_exchange_name
        self.route_q2 = route_q2
        self.route_q4 = route_q4

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.players_exchange_name, self._callback)

        # setup output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

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
