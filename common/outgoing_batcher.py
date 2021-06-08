from common.batch_encoder_decoder import BatchEncoderDecoder
from common.shard_key_getter import ShardKeyGetter
import logging

class OutgoingBatcher:
    def __init__(self, rabbit_channel, reducers_amount, batch_size, output_exchange_name):
        self.all_outgoing_batches = {}
        self.shard_key_getter = ShardKeyGetter(reducers_amount)
        self.channel = rabbit_channel
        self.max_batch_size = batch_size
        self.output_exchange_name = output_exchange_name

    def add_to_batch(self, item):
        shard_key = self.shard_key_getter.get_key_for_str(item['match'])
        
        batch = self.all_outgoing_batches.get(shard_key, [])
        batch.append(item)
        self.all_outgoing_batches[shard_key] = batch

    def publish_if_full(self):
        for shard_key,batch in self.all_outgoing_batches.items():
            if len(batch) >= self.max_batch_size:
                logging.info(f'SHARDING EXCHANGE: Announcing batch for shard key {shard_key}')
                serialized = BatchEncoderDecoder.encode_batch(batch)
                self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=serialized)
