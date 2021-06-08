from common.config_setup import setup
from src.shard_exchange_controller import ShardExchangeController

def main():
	config_params = setup('config.ini',
		{'PLAYERS_RATINGS_EXCHANGE_NAME': False,
		'OUTPUT_EXCHANGE_NAME': False,
		'RABBIT_IP': False,
		'ROUTING_KEY_QUERY2': False,
		'REDUCERS_AMOUNT': True,
		'BATCH_SIZE': True})
	rabbit_ip = config_params['RABBIT_IP']
	ratings_exchange_name = config_params['PLAYERS_RATINGS_EXCHANGE_NAME']
	output_exchange_name = config_params['OUTPUT_EXCHANGE_NAME']
	route_q2 = config_params['ROUTING_KEY_QUERY2']
	reducers_amount = config_params['REDUCERS_AMOUNT']
	batch_size = config_params['BATCH_SIZE']

	controller = ShardExchangeController(rabbit_ip, ratings_exchange_name, output_exchange_name, route_q2, reducers_amount, batch_size)
	controller.run()

if __name__== "__main__":
	main()
