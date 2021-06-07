import pika
import logging

class FanoutController:
    def __init__(self, rabbit_ip, sources_queue_name, exchange_name):
        self.exchange_name = exchange_name
        self.sources_queue_name = sources_queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.sources_queue_name)
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')

        self.channel.basic_consume(
            queue=self.sources_queue_name, on_message_callback=self._callback, auto_ack=True)

    def run(self):
        logging.info('FANOUT: Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        self.connection.close()

    def _callback(self, ch, method, properties, body):
        logging.info(f"FANOUT: Received batch {body[:25]}...")
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=body)
