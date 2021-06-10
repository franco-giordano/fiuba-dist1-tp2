import logging
from common.models.civilizations_grouper import CivilizationsGrouper
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils

class ShardedGrouperController:
    def __init__(self, rabbit_ip, shard_exchange_name, output_queue_name, assigned_shard_key, aggregator):
        self.shard_exchange_name = shard_exchange_name
        self.assigned_shard_key = assigned_shard_key

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name, assigned_shard_key, self._callback)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

        self.civ_grouper = CivilizationsGrouper(self.channel, output_queue_name, aggregator)

    def run(self):
        logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"SHARDED GROUPER {self.assigned_shard_key}: Received sentinel! Flushing and shutting down...")
            self.civ_grouper.received_sentinel()
            # TODO: shutdown my node
            return

        joined_match = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f'SHARDED GROUPER {self.assigned_shard_key}: Received joined match {body[:25]}...')

        self.civ_grouper.add_joined_match(joined_match)
