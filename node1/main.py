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

	for i in range(100):
		channel.basic_publish(exchange='', routing_key='hello', body=f'{msg} {i}!')
		logging.info(" [x] Sent 'Hello World {}!'".format(i))
		time.sleep(1)

	connection.close()

if __name__== "__main__":
	main()
