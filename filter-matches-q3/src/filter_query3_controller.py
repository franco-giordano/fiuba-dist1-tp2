from common.encoders.match_encoder_decoder import MatchEncoderDecoder
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query3 import FilterQuery3
from common.models.shard_key_getter import ShardKeyGetter
from common.utils.rabbit_utils import RabbitUtils
import logging

class FilterQuery3Controller:
    def __init__(self, rabbit_ip, matches_exchange_name, output_exchange_name, reducers_amount, routing_key):
        self.matches_exchange_name = matches_exchange_name
        self.output_exchange_name = output_exchange_name
        self.shard_key_getter = ShardKeyGetter(reducers_amount)

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.matches_exchange_name, routing_key, self._callback)

        # output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

        self.filter = FilterQuery3()

    def run(self):
        logging.info('FILTER QUERY3: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER QUERY3: Received sentinel! Shutting down...")
            # TODO: shutdown my node
            return

        match = MatchEncoderDecoder.decode_bytes(body)

        if self.filter.should_pass(match):
            shard_key = self.shard_key_getter.get_key_for_str(match['token'])
            logging.info(f'FILTER QUERY3: Sending to output queue the passing match {match}')
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=body)
