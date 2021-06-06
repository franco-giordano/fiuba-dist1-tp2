import pika
from common.dict_encoder_decoder import DictEncoderDecoder

class Query1Controller:
    def __init__(self, rabbit_ip, input_queue_name, output_queue_name):
        self.input_queue_name = input_queue_name
        self.output_queue_name = output_queue_name

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_ip))

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.input_queue_name)

        self.channel.basic_consume(
            queue=self.input_queue_name, on_message_callback=self._callback, auto_ack=True)


    def run(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        deser = DictEncoderDecoder.decode_bytes(body)
        print(f" [x] Received {deser}")
