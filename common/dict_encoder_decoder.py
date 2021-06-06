import json

class DictEncoderDecoder:
    @staticmethod
    def decode_bytes(bytes_recv):
        return json.loads(bytes_recv.decode('utf-8'))

    @staticmethod
    def encode_dict(dict_recv):
        return json.dumps(dict_recv).encode('utf-8')
