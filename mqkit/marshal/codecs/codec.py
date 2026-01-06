from abc import ABCMeta, abstractmethod
from typing import Any, Dict


class Codec(metaclass=ABCMeta):
    @abstractmethod
    def decode(self: "Codec", data: bytes) -> Dict[str, Any]:
        raise NotImplementedError()

    @abstractmethod
    def encode(self: "Codec", data: Dict[str, Any]) -> bytes:
        raise NotImplementedError()
