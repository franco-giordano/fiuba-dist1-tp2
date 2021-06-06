import json

class DictEncoderDecoder:
    @staticmethod
    def decode_bytes(bytes_recv):
        parsed = json.loads(bytes_recv.decode('utf-8'))
        parsed['winning_team'] = int(parsed['winning_team'])
        parsed['patch'] = int(parsed['patch'])
        parsed['num_players'] = int(parsed['num_players'])
        parsed['mirror'] = parsed['mirror'] == 'True'
        parsed['average_rating'] = int(parsed['average_rating']) if parsed['average_rating'] else 0
        parsed['duration'] = parse_duration_as_seconds(parsed['duration'])
        return parsed

    @staticmethod
    def encode_dict(dict_recv):
        return json.dumps(dict_recv).encode('utf-8')

def parse_duration_as_seconds(dur):
    by_comma = dur.split(', ')
    if len(by_comma) == 1:
        nums = list(map(lambda x: int(x), by_comma[0].split(':')))
        return nums[0]*3600 + nums[1]*60 + nums[2]
    else:
        days = int(by_comma[0].split(' ')[0])
        nums = list(map(lambda x: int(x), by_comma[1].split(':')))
        return days*3600*24 + nums[0]*3600 + nums[1]*60 + nums[2]
