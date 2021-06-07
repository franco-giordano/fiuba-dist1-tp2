from common.config_setup import setup
from common.fanout_controller import FanoutController

def main():
	config_params = setup('config.ini', {'INPUT_PLAYERS_QUEUE': False, 'PLAYERS_EXCHANGE_NAME': False, 'RABBIT_IP': False})
	rabbit_ip = config_params['RABBIT_IP']
	players_queue = config_params['INPUT_PLAYERS_QUEUE']
	exchange_name = config_params['PLAYERS_EXCHANGE_NAME']

	fanout = FanoutController(rabbit_ip, players_queue, exchange_name)
	fanout.run()

if __name__== "__main__":
	main()
