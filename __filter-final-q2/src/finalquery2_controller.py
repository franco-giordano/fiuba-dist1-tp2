from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from src.filter_query2 import FilterQuery2
from common.utils.rabbit_utils import RabbitUtils
import logging

class FinalQuery2Controller:
    def __init__(self, rabbit_ip, grouped_queue_name, output_queue_name):
        self.grouped_queue_name = grouped_queue_name
        self.output_queue_name = output_queue_name

        self.connection, self.channel = RabbitUtils.setup_connection_with_channel(rabbit_ip)

        # input exchange
        RabbitUtils.setup_input_queue(self.channel, self.grouped_queue_name, self._callback)

        # output queue
        RabbitUtils.setup_queue(self.channel, self.output_queue_name)

        self.filter = FilterQuery2()

    def run(self):
        logging.info('FILTER QUERY2: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        if BatchEncoderDecoder.is_encoded_sentinel(body):
            logging.info(f"FILTER QUERY2: Received sentinel! Shutting down...")
            # TODO: shutdown my node
            return

        players = BatchEncoderDecoder.decode_bytes(body)
        logging.info(f"FILTER QUERY2: Received players {players}")

        if self.filter.should_pass(players):
            logging.info(f'FILTER QUERY2: Sending to output queue the passing players {players}')
            serialized = BatchEncoderDecoder.encode_batch(players)
            self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)
