"""
module mqkit.marshal.codecs.jsoncodec

Defines the JsonCodec class for encoding and decoding JSON data.
"""

import json
from typing import Any, Dict

from .codec import Codec
from ...errors import DecodeError, EncodeError


class JsonCodec(Codec):
    """
    class JsonCodec

    A codec for encoding and decoding JSON data.
    """

    _encoding: str

    def __init__(self: "JsonCodec", encoding: str = "utf-8") -> None:
        self._encoding = encoding

    @property
    def content_type(self: "JsonCodec") -> str:
        return "application/json"

    def decode(self: "JsonCodec", data: bytes) -> Dict[str, Any]:
        try:
            return json.loads(
                data.decode(self._encoding),
            )
        except json.JSONDecodeError as exc:
            raise DecodeError("Failed to decode JSON data") from exc

    def encode(self: "JsonCodec", data: object) -> bytes:
        try:
            return json.dumps(data).encode(self._encoding)
        except (TypeError, ValueError) as exc:
            raise EncodeError("Failed to encode data to JSON") from exc
