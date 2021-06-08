import pika
import logging
from common.outgoing_batcher import OutgoingBatcher
from common.batch_encoder_decoder import BatchEncoderDecoder

class ShardExchangeController:
    def __init__(self, rabbit_ip, ratings_exchange_name, output_exchange_name, route_q2, reducers_amount, batch_size):
        self.ratings_exchange_name = ratings_exchange_name
        self.route_q2 = route_q2

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))
        self.channel = self.connection.channel()

        # setup input exchange
        self.channel.exchange_declare(exchange=self.ratings_exchange_name, exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.ratings_exchange_name, queue=queue_name, routing_key=self.route_q2)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self._callback, auto_ack=True)

        # setup output queue
        self.channel.exchange_declare(exchange=output_exchange_name, exchange_type='direct')

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
