"""
module mqkit.marshal.codecs.jsoncodec

Defines the JsonCodec class for encoding and decoding JSON data.
"""

import json
from typing import Any, Dict

from .codec import Codec


class JsonCodec(Codec):
    """
    class JsonCodec

    A codec for encoding and decoding JSON data.
    """

    _encoding: str

    def __init__(self: "JsonCodec", encoding: str = "utf-8") -> None:
        self._encoding = encoding

    def decode(self: "JsonCodec", data: bytes) -> Dict[str, Any]:
        return json.loads(
            data.decode(self._encoding),
        )

    def encode(self: "JsonCodec", data: object) -> bytes:
        return json.dumps(data).encode(self._encoding)
