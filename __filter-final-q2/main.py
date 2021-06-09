from common.utils.config_setup import setup
from src.finalquery2_controller import FinalQuery2Controller

def main():
	config_params = setup('config.ini', {'GROUPED_QUEUE_NAME': False, 'OUTPUT_QUEUE': False, 'RABBIT_IP': False})
	rabbit_ip = config_params['RABBIT_IP']
	grouped_queue_name = config_params['GROUPED_QUEUE_NAME']
	output_queue = config_params['OUTPUT_QUEUE']

	controller2 = FinalQuery2Controller(rabbit_ip, grouped_queue_name, output_queue)
	controller2.run()

if __name__== "__main__":
	main()
