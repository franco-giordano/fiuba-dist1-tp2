from common.config_setup import setup
import pika

def main():
	config_params = setup('config.ini', {'INPUT_MATCHES_QUEUE': False, 'MATCHES_EXCHANGE_NAME': False, 'RABBIT_IP': False})
	rabbit_ip = config_params['RABBIT_IP']
	matches_queue = config_params['INPUT_MATCHES_QUEUE']
	exchange_name = config_params['MATCHES_EXCHANGE_NAME']

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_ip))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    message = 
    channel.basic_publish(exchange=exchange_name, routing_key='', body=message)

    
    connection.close()

if __name__== "__main__":
	main()
