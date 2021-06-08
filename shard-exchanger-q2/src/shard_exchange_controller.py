import logging
from common.models.outgoing_batcher import OutgoingBatcher
from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.utils.rabbit_utils import RabbitUtils

class ShardExchangeController:
    def __init__(self, rabbit_ip, ratings_exchange_name, output_exchange_name, route_q2, reducers_amount, batch_size):
        self.ratings_exchange_name = ratings_exchange_name
        self.route_q2 = route_q2

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # setup input exchange
        RabbitUtils.setup_input_direct_exchange(self.channel, self.ratings_exchange_name, self.route_q2, self._callback)

        # setup output exchange
        RabbitUtils.setup_output_direct_exchange(self.channel, output_exchange_name)

        self.outgoing_batcher = OutgoingBatcher(self.channel, reducers_amount, batch_size, output_exchange_name)

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
