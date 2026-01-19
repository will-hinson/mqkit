from mqkit.errors import DecodeError, EncodeError
from mqkit.marshal.codecs import YamlCodec

import pytest


@pytest.fixture
def yaml_codec() -> YamlCodec:
    return YamlCodec()


def test_yaml_codec_content_type(yaml_codec: YamlCodec) -> None:
    assert yaml_codec.content_type == "application/yaml"


def test_yaml_codec_decode(yaml_codec: YamlCodec) -> None:
    assert yaml_codec.decode(b'{"key": "value"}') == {"key": "value"}


def test_yaml_codec_encode(yaml_codec: YamlCodec) -> None:
    assert yaml_codec.encode({"key": "value"}).strip() == b"key: value"


def test_yaml_codec_decode_invalid(yaml_codec: YamlCodec) -> None:
    with pytest.raises(DecodeError):
        yaml_codec.decode(b"invalid_yaml: [unclosed_list")


def test_yaml_codec_encode_invalid(yaml_codec: YamlCodec) -> None:
    class NonSerializable:
        pass

    with pytest.raises(EncodeError):
        print(yaml_codec.encode({"a": NonSerializable()}))
