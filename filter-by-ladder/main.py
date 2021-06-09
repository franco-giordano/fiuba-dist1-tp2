#!/usr/bin/env python3

from common.utils.config_setup import setup
from src.ladder_filter_controller import LadderFilterController

def main():
	config_params = setup('config.ini',
		{'MATCHES_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'ROUTE_1v1': False,
		'ROUTE_TEAM': False})
	rabbit_ip = config_params['RABBIT_IP']
	matches_exchange_name = config_params['MATCHES_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	route_1v1 = config_params['ROUTE_1v1']
	route_team = config_params['ROUTE_TEAM']

	controller = LadderFilterController(rabbit_ip, matches_exchange_name, output_exchange_name, route_1v1, route_team)
	controller.run()

if __name__== "__main__":
	main()
