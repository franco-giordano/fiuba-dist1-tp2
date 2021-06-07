#!/usr/bin/env python3

from common.config_setup import setup
from src.rating_filter_controller import RatingFilterController

def main():
	config_params = setup('config.ini',
		{'PLAYERS_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'ROUTING_KEY_QUERY2': False,
		'ROUTING_KEY_QUERY4': False})
	rabbit_ip = config_params['RABBIT_IP']
	players_exchange_name = config_params['PLAYERS_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	route_q2 = config_params['ROUTING_KEY_QUERY2']
	route_q4 = config_params['ROUTING_KEY_QUERY4']

	controller = RatingFilterController(rabbit_ip, players_exchange_name, output_exchange_name, route_q2, route_q4)
	controller.run()

if __name__== "__main__":
	main()
