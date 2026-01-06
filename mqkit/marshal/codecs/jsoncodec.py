import json
from typing import Any, Dict

from .codec import Codec


class JsonCodec(Codec):
    _encoding: str

    def __init__(self: "JsonCodec", encoding: str = "utf-8") -> None:
        self._encoding = encoding

    def decode(self: "JsonCodec", data: bytes) -> Dict[str, Any]:
        return json.loads(
            data.decode(self._encoding),
        )

    def encode(self: "JsonCodec", data: object) -> bytes:
        return json.dumps(data).encode(self._encoding)
