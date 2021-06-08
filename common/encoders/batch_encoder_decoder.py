from common.encoders.obj_encoder_decoder import ObjectEncoderDecoder

class BatchEncoderDecoder:
    @staticmethod
    def encode_batch(batch):
        return ObjectEncoderDecoder.encode_obj(batch)

    @staticmethod
    def decode_bytes(bytes_recv):
        return ObjectEncoderDecoder.decode_bytes(bytes_recv)
