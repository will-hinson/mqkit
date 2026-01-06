from abc import ABCMeta, abstractmethod
from typing import Any, Optional

from .codecs import Codec


class Serializer(metaclass=ABCMeta):
    _codec: Codec

    def __init__(self: "Serializer", codec: Codec) -> None:
        self._codec = codec

    @abstractmethod
    def deserialize(self: "Serializer", data: bytes) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def serialize(self: "Serializer", data: Any) -> Optional[bytes]:
        raise NotImplementedError()
