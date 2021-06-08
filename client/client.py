from common.utils.config_setup import setup
from src.csv_dispatcher import CSVDispatcher

config_params = setup('config.ini',
    {'INPUT_MATCHES_QUEUE': False,
    'RABBIT_IP': False,
    'INPUT_PLAYERS_QUEUE': False,
    'MATCHES_PATH': False,
    'PLAYERS_PATH': False,
    'BATCH_SIZE': True})

rabbit_ip = config_params['RABBIT_IP']
matches_queue = config_params['INPUT_MATCHES_QUEUE']
matches_path = config_params['MATCHES_PATH']
players_queue = config_params['INPUT_PLAYERS_QUEUE']
players_path = config_params['PLAYERS_PATH']
batch_size = config_params['BATCH_SIZE']

dispatcher = CSVDispatcher(rabbit_ip, matches_queue, matches_path, players_queue, players_path, batch_size)
dispatcher.run()
