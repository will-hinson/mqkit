from mqkit.errors import DecodeError
from mqkit.marshal.codecs import JsonCodec

import pytest


@pytest.fixture
def json_codec() -> JsonCodec:
    return JsonCodec()


def test_json_codec_content_type(json_codec: JsonCodec) -> None:
    assert json_codec.content_type == "application/json"


def test_json_codec_decode(json_codec: JsonCodec) -> None:
    assert json_codec.decode(b'{"key": "value"}') == {"key": "value"}


def test_json_codec_encode(json_codec: JsonCodec) -> None:
    assert json_codec.encode({"key": "value"}).replace(b" ", b"") == b'{"key":"value"}'


def test_json_codec_decode_invalid(json_codec: JsonCodec) -> None:
    with pytest.raises(DecodeError):
        json_codec.decode(b'{"key": value}')
