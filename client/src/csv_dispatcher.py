import pika
import logging
import csv
from multiprocessing import Process
from common.player_encoder_decoder import PlayerEncoderDecoder
from common.match_encoder_decoder import MatchEncoderDecoder
from common.batch_encoder_decoder import BatchEncoderDecoder

class CSVDispatcher:
    def __init__(self, rabbit_ip, matches_queue, matches_path, players_queue, players_path, batch_size):
        self.rabbit_ip = rabbit_ip

        self.BATCH_SIZE = batch_size

        self.matches_proc = Process(target=self.upload_csv, args=(matches_path, matches_queue, MatchEncoderDecoder))
        self.players_proc = Process(target=self.upload_csv, args=(players_path, players_queue, PlayerEncoderDecoder))

    def run(self):
        self.matches_proc.start()
        self.players_proc.start()

        self.matches_proc.join()
        self.players_proc.join()

    def upload_csv(self, csv_path, queue_name, decoder):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_ip))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)

        with open(csv_path, newline='') as csvf:
            reader = csv.DictReader(csvf)
            batch = []
            count = 0
            for row_dict in reader:
                parsed_dict = decoder.parse_dict(row_dict)
                batch.append(parsed_dict)
                count += 1

                if count >= self.BATCH_SIZE:
                    serialized = BatchEncoderDecoder.encode_batch(batch)
                    channel.basic_publish(exchange='', routing_key=queue_name, body=serialized)
                    logging.info(f"{queue_name}: Sent batch {serialized[:25]}...")
                    batch = []
                    count = 0
                    # time.sleep(5)

            if count > 0:
                serialized = BatchEncoderDecoder.encode_batch(batch)
                channel.basic_publish(exchange='', routing_key=queue_name, body=serialized)
                logging.info(f"{queue_name}: Sent last missing batch {serialized[:25]}...")
            
        connection.close()
