from mqkit.marshal.codecs import RawCodec


def test_rawcodec() -> None:
    codec = RawCodec()

    assert codec.content_type == "application/octet-stream"

    original_data = b"sample raw data"
    encoded_data = codec.encode(original_data)
    decoded_data = codec.decode(encoded_data)

    assert encoded_data == original_data
    assert decoded_data == original_data
