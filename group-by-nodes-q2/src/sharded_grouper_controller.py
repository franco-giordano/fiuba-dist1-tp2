import logging
from common.models.outgoing_batcher import OutgoingBatcher
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils

class ShardedGrouperController:
    def __init__(self, rabbit_ip, shard_exchange_name, output_queue_name, assigned_shard_key):
        self.shard_exchange_name = shard_exchange_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.shard_exchange_name, assigned_shard_key, self._callback)

        # setup output queue
        RabbitUtils.setup_queue(self.channel, output_queue_name)

    def run(self):
        logging.info('SHARD EXCHANGE: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        batch = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"SHARD EXCHANGE: Received batch {body[:25]}...")

        for player in batch:
            self.outgoing_batcher.add_to_batch(player)

        self.outgoing_batcher.publish_if_full()
