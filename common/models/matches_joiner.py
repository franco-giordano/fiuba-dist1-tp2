from common.encoders.batch_encoder_decoder import BatchEncoderDecoder
from common.models.shard_key_getter import ShardKeyGetter
import logging

class MatchesJoiner:
    def __init__(self, channel, output_exchange_name, next_reducers_amount):
        self.channel = channel
        self.output_exchange_name = output_exchange_name
        self.current_matches = {}
        self.shard_key_getter = ShardKeyGetter(next_reducers_amount)

    def add_players_batch(self, batch):
        for p in batch:
            self._add_player(p)

    def _add_player(self, player):
        tkn = player['match']
        joined_info = self.current_matches.get(tkn, [None, []])
        joined_info[1].append(player)
        self._send_or_store(joined_info, tkn)

    def _add_match(self, match):
        tkn = match['token']
        joined_info = self.current_matches.get(tkn, [None, []])
        assert(joined_info[0] is None)
        joined_info[0] = match
        self._send_or_store(joined_info, tkn)

    def _send_or_store(self, joined_info, token):
        if self._is_match_complete(joined_info):
            shard_key = self.shard_key_getter.get_key_for_str(token)
            serialized = BatchEncoderDecoder.encode_batch(joined_info)
            logging.info(f'MATCHES JOINER: Finished joining match {joined_info}, sending to shard key {shard_key}')
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=shard_key, body=serialized)
            del self.current_matches[token]
        else:
            self.current_matches[token] = joined_info

    def add_matches_batch(self, batch):
        for m in batch:
            self._add_match(m)

    def _is_match_complete(self, joined_info):
        assert(len(joined_info[1])<=joined_info[0]['num_players'] if joined_info[0] is not None else True)
        return joined_info[0] is not None and len(joined_info[1])==joined_info[0]['num_players']

    def received_sentinel(self):
        logging.info(f'MATCHES JOINER: Propagating sentinel to group-by nodes...')
        sentinel = BatchEncoderDecoder.create_encoded_sentinel()
        all_keys = self.shard_key_getter.generate_all_shard_keys()
        for key in all_keys:
            self.channel.basic_publish(exchange=self.output_exchange_name, routing_key=key, body=sentinel)

    # def flush_1v1_matches(self):
    #     for tkn,players in self.current_matches.items():
    #         if players and len(players) == 2 and self.last_filter.should_pass(players):
    #             logging.info(f'MATCH 1v1 GROUPER: Announcing players for match {tkn}')
    #             serialized = BatchEncoderDecoder.encode_batch(players)
    #             self.channel.basic_publish(exchange='', routing_key=self.output_queue_name, body=serialized)
