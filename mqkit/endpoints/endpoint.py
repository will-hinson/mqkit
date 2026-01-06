from abc import ABCMeta, abstractmethod
import functools
import inspect
from typing import Any, Callable, Dict, NoReturn

from ..marshal import QueueMessage, Serializer, TypelessSerializer
from ..marshal.codecs import Codec, CodecType, JsonCodec


class Endpoint(metaclass=ABCMeta):
    _queue_name: str
    __target: Callable[..., Any]

    def __init__(
        self: "Endpoint", queue_name: str, target: Callable, codec_type: CodecType
    ) -> None:
        self._queue_name = queue_name
        self.__target = self._wrap_with_decode(
            target,
            codec_type=codec_type,
        )

    @abstractmethod
    def handle_message(self: "Endpoint", message: QueueMessage) -> None:
        raise NotImplementedError()

    @property
    @abstractmethod
    def qualname(self: "Endpoint") -> str:
        raise NotImplementedError()

    @property
    def target(self: "Endpoint") -> Callable[..., Any]:
        return self.__target

    def __call__(self, /) -> NoReturn:
        raise NotImplementedError(
            f"{self.target.__name__}() should not be called directly. Use App.run() instead"
        )

    def _make_codec(self: "Endpoint", codec_type: CodecType) -> Codec:
        if codec_type == CodecType.JSON:
            return JsonCodec()

        raise NotImplementedError()

    def _make_serializer(
        self: "Endpoint", func: Callable, codec_type: CodecType
    ) -> Serializer:
        codec: Codec = self._make_codec(codec_type)

        signature: inspect.Signature = inspect.signature(func)
        if len(signature.parameters) != 2:
            raise ValueError(
                f"Function {func.__name__}() must accept exactly two parameters"
            )

        # check for no type annotations. if this is the case, infer no return value
        if func.__annotations__ == {}:
            return TypelessSerializer(codec=codec)

        raise ValueError(
            "Unable to infer serializer type from function annotations for function "
            f"{func.__name__}()"
        )

    def _wrap_with_decode(
        self: "Endpoint", func: Callable, codec_type: CodecType
    ) -> Callable[..., Any]:
        serializer: Serializer = self._make_serializer(
            func=func,
            codec_type=codec_type,
        )

        @functools.wraps(func)
        def _deserialize_wrapper(
            *,
            message: bytes,
            attributes: Dict[str, Any],
        ) -> Any:
            return serializer.serialize(
                func(
                    serializer.deserialize(message),
                    attributes,
                )
            )

        return _deserialize_wrapper
