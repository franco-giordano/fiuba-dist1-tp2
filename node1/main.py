#!/usr/bin/env python3

from common.config_setup import setup
import logging
import pika
import time

def main():
	config_params = setup('config.ini', {'MSG': ('msg', False)})
	msg = config_params['msg']

	connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))

	channel = connection.channel()
	channel.queue_declare(queue='hello')

	def callback(ch, method, properties, body):
		print(f" [x] Received {body}")

	channel.basic_consume(
		queue='hello', on_message_callback=callback, auto_ack=True)

	print(' [*] Waiting for messages. To exit press CTRL+C')
	channel.start_consuming()

if __name__== "__main__":
	main()
