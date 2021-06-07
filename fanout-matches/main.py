from common.config_setup import setup
from common.fanout_controller import FanoutController
import pika

def main():
	config_params = setup('config.ini', {'INPUT_MATCHES_QUEUE': False, 'MATCHES_EXCHANGE_NAME': False, 'RABBIT_IP': False})
	rabbit_ip = config_params['RABBIT_IP']
	matches_queue = config_params['INPUT_MATCHES_QUEUE']
	exchange_name = config_params['MATCHES_EXCHANGE_NAME']

	fanout = FanoutController(rabbit_ip, matches_queue, exchange_name)
	fanout.run()

if __name__== "__main__":
	main()
