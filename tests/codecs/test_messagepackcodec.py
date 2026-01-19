from typing import Any, Dict

from mqkit.errors import DecodeError, EncodeError
from mqkit.marshal.codecs import MessagePackCodec

import pytest


@pytest.fixture
def codec() -> MessagePackCodec:
    return MessagePackCodec()


def test_messagepack_codec_content_type(codec: MessagePackCodec) -> None:
    assert codec.content_type == "application/msgpack"


def test_messagepack_codec_decode(codec: MessagePackCodec) -> None:
    data: bytes = b"\x83\xa3key\xa5value\xa6number*\xa4list\x93\x01\x02\x03"
    assert codec.decode(data) == {"key": "value", "number": 42, "list": [1, 2, 3]}


def test_messagepack_codec_encode(codec: MessagePackCodec) -> None:
    data: Dict[str, Any] = {"key": "value", "number": 42, "list": [1, 2, 3]}
    assert (
        codec.encode(data) == b"\x83\xa3key\xa5value\xa6number*\xa4list\x93\x01\x02\x03"
    )


def test_messagepack_codec_decode_invalid(codec: MessagePackCodec) -> None:
    invalid_data: bytes = b"\x81\xa3key"  # Incomplete MessagePack data
    with pytest.raises(DecodeError):
        codec.decode(invalid_data)


def test_messagepack_codec_encode_invalid(codec: MessagePackCodec) -> None:
    invalid_data: Any = set([1, 2, 3])  # Sets are not serializable in MessagePack
    with pytest.raises(EncodeError):
        codec.encode(invalid_data)


def test_messagepack_codec_decode_non_dict(codec: MessagePackCodec) -> None:
    data: bytes = b"\x92\x01\x02"  # MessagePack array [1, 2]

    with pytest.raises(DecodeError):
        codec.decode(data)
