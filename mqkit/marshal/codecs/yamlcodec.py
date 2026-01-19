"""
module mqkit.marshal.codecs.yamlcodec

Defines the YamlCodec class for encoding and decoding YAML data in message queues.
"""

import yaml

from .codec import Codec
from ...errors import DecodeError, EncodeError


class YamlCodec(Codec):
    """
    class YamlCodec

    A codec for encoding and decoding YAML data.
    """

    _encoding: str

    def __init__(self: "YamlCodec", encoding: str = "utf-8") -> None:
        self._encoding = encoding

    @property
    def content_type(self: "YamlCodec") -> str:
        return "application/yaml"

    def decode(self: "YamlCodec", data: bytes) -> dict:
        try:
            return yaml.safe_load(
                data.decode(self._encoding),
            )
        except yaml.YAMLError as exc:
            raise DecodeError("Failed to decode YAML data") from exc

    def encode(self: "YamlCodec", data: dict) -> bytes:
        try:
            return yaml.safe_dump(data).encode(self._encoding)
        except yaml.YAMLError as exc:
            raise EncodeError("Failed to encode YAML data") from exc
