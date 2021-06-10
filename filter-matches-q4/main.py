from common.utils.config_setup import setup
from common.controllers.filter_matches_controller import FilterMatchesController
from src.filter_query4 import FilterQuery4

def main():
	config_params = setup('config.ini',
		{'MATCHES_BY_LADDER_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'NEXT_REDUCERS_AMOUNT': True,
		'BATCH_SIZE': True,
		'TEAM_ROUTING_KEY': False})
	rabbit_ip = config_params['RABBIT_IP']
	matches_exchange_name = config_params['MATCHES_BY_LADDER_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	reducers_amount = config_params['NEXT_REDUCERS_AMOUNT']
	input_routing_key = config_params['TEAM_ROUTING_KEY']
	batch_size = config_params['BATCH_SIZE']

	match_filter = FilterQuery4()
	controller = FilterMatchesController(rabbit_ip, matches_exchange_name, \
		output_exchange_name, reducers_amount, input_routing_key, batch_size, match_filter)
	controller.run()

if __name__== "__main__":
	main()
