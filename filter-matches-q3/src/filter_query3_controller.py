from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query3 import FilterQuery3
from common.models.sharded_outgoing_batcher import ShardedOutgoingBatcher
from common.utils.rabbit_utils import RabbitUtils
import logging

class FilterQuery3Controller:
    def __init__(self, rabbit_ip, matches_exchange_name, output_exchange_name, reducers_amount, routing_key, batch_size):
        self.matches_exchange_name = matches_exchange_name
        self.output_exchange_name = output_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.matches_exchange_name, routing_key, self._callback)

        # output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, self.output_exchange_name)

        self.filter = FilterQuery3()
        self.sharded_outgoing_batcher = ShardedOutgoingBatcher(self.channel, reducers_amount, batch_size, output_exchange_name, tkn_key='token')

    def run(self):
        logging.info('FILTER QUERY3: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER QUERY3: Received sentinel! Shutting down...")
            self.sharded_outgoing_batcher.received_sentinel()
            # TODO: shutdown my node
            return

        batch = BatchEncoderDecoder.decode_bytes(body)

        for match in batch:
            if self.filter.should_pass(match):
                self.sharded_outgoing_batcher.add_to_batch(match)

        self.sharded_outgoing_batcher.publish_if_full()
