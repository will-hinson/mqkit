import yaml

from .codec import Codec


class YamlCodec(Codec):
    _encoding: str

    def __init__(self: "YamlCodec", encoding: str = "utf-8") -> None:
        self._encoding = encoding

    def decode(self: "YamlCodec", data: bytes) -> dict:
        return yaml.safe_load(
            data.decode(self._encoding),
        )

    def encode(self: "YamlCodec", data: dict) -> bytes:
        return yaml.safe_dump(data).encode(self._encoding)
