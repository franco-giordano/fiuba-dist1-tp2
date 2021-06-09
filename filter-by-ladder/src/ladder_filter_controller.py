from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.encoders.match_encoder_decoder import MatchEncoderDecoder
from src.filter_ladder import FilterLadder
from common.utils.rabbit_utils import RabbitUtils
import logging

class LadderFilterController:
    def __init__(self, rabbit_ip, matches_exchange_name, output_exchange_name, route_1v1, route_team):
        self.route_1v1 = route_1v1
        self.route_team = route_team
        self.matches_exchange_name = matches_exchange_name
        self.output_exchange_name = output_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_fanout_exchange(self.channel, self.matches_exchange_name, self._callback)

        # output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

        self.filter = FilterLadder(route_1v1, route_team)

    def run(self):
        logging.info('FILTER BY LADDER: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER BY LADDER: Received sentinel! Shutting down...")
            # TODO: shutdown my node
            return

        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"FILTER BY LADDER: Received batch {body[:25]}...")

        # TODO: change to batched publish? takes too long
        for match in batch:
            route = self.filter.select_route(match)
            logging.info(f'FILTER BY LADDER: Sending to output exchange the match {match} with route {route}')
            serialized = MatchEncoderDecoder.encode_match(match)
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=route, body=serialized)
    