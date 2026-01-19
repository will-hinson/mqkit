"""
module mqkit.endpoints.endpoint

Abstract base class for message queue endpoints.
"""

from abc import ABCMeta, abstractmethod
from copy import copy
import functools
import inspect
from typing import Any, Callable, Dict, NoReturn, Optional, Type

from ..marshal import Forward, QueueMessage, Serializer, TypelessSerializer
from ..marshal.codecs import Codec, CodecType, JsonCodec, YamlCodec

_codec_type_to_class: Dict[CodecType, Type[Codec]] = {
    CodecType.JSON: JsonCodec,
    CodecType.YAML: YamlCodec,
}


class Endpoint(metaclass=ABCMeta):
    """
    class Endpoint

    Abstract base class for message queue endpoints.
    """

    _queue_name: str
    _target: Callable[..., Any]

    def __init__(
        self: "Endpoint", queue_name: str, target: Callable, codec_type: CodecType
    ) -> None:
        self._queue_name = queue_name
        self._target = self._wrap_with_decode(
            target,
            codec_type=codec_type,
        )

    @abstractmethod
    def handle_message(self: "Endpoint", message: QueueMessage) -> Optional[Forward]:
        """
        Handles an incoming message, processes it, and optionally returns a result to
        be forwarded to another message target (queue, topic, etc.)

        Args:
            message (QueueMessage): The incoming message to be processed.

        Returns:
            Optional[Forward]: An optional Forward object indicating where to forward the result.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @property
    @abstractmethod
    def qualname(self: "Endpoint") -> str:
        """
        Returns the qualified name of the endpoint.

        Returns:
            str: The qualified name of the endpoint.
        """

        raise NotImplementedError()

    @property
    def queue_name(self: "Endpoint") -> str:
        """
        Property that returns the name of the queue associated with this endpoint.

        Returns:
            str: The name of the queue.
        """

        return copy(self._queue_name)

    @property
    def target(self: "Endpoint") -> Callable[..., Any]:
        """
        Property that returns the target function associated with this endpoint.

        Returns:
            Callable[..., Any]: The target function.
        """

        return self._target

    def __call__(self, /) -> NoReturn:
        raise ValueError(
            f"{self.target.__name__}() should not be called directly. Use App.run() instead"
        )

    def _make_codec(self: "Endpoint", codec_type: CodecType) -> Codec:
        if codec_type not in _codec_type_to_class:
            raise NotImplementedError(f"Unsupported codec type: {codec_type}")

        return _codec_type_to_class[codec_type]()

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
