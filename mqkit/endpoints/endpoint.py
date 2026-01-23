"""
module mqkit.endpoints.endpoint

Abstract base class for message queue endpoints.
"""

from abc import ABCMeta, abstractmethod
import asyncio
import functools
import inspect
from typing import Any, Callable, Dict, NoReturn, Optional, Type

from ..errors import FunctionSignatureError
from ..marshal import (
    Forward,
    QueueMessage,
    ReturnTypeSerializer,
    Serializer,
    TypelessSerializer,
)
from ..marshal.codecs import Codec, CodecType, JsonCodec, MessagePackCodec, YamlCodec

_codec_type_to_class: Dict[CodecType, Type[Codec]] = {
    CodecType.JSON: JsonCodec,
    CodecType.MESSAGEPACK: MessagePackCodec,
    CodecType.YAML: YamlCodec,
}


class Endpoint(metaclass=ABCMeta):
    """
    class Endpoint

    Abstract base class for message queue endpoints.
    """

    _is_async: bool = False
    _target: Callable[..., Any]

    def __init__(self: "Endpoint", target: Callable, codec_type: CodecType) -> None:
        self._is_async = asyncio.iscoroutinefunction(target)
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
    def is_async(self: "Endpoint") -> bool:
        """
        Property that indicates whether the endpoint's target function is asynchronous.

        Returns:
            bool: True if the target function is asynchronous, False otherwise.
        """

        return self._is_async

    @property
    def is_auto_delete(self: "Endpoint") -> bool:
        """
        Property that indicates whether the endpoint is auto-delete.

        Returns:
            bool: True if the endpoint is auto-delete, False otherwise.
        """

        raise NotImplementedError(
            f"Endpoint of type {type(self).__name__} does not implement is_auto_delete()"
        )

    @property
    def is_persistent(self: "Endpoint") -> bool:
        """
        Property that indicates whether the endpoint is persistent.

        Returns:
            bool: True if the endpoint is persistent, False otherwise.
        """

        raise NotImplementedError(
            f"Endpoint of type {type(self).__name__} does not implement is_persistent()"
        )

    @property
    @abstractmethod
    def qualname(self: "Endpoint") -> str:  # pragma: no cover
        """
        Returns the qualified name of the endpoint.

        Returns:
            str: The qualified name of the endpoint.
        """

        raise NotImplementedError()

    @property
    @abstractmethod
    def queue_name(self: "Endpoint") -> str:
        """
        Property that returns the name of the queue associated with this endpoint.

        Returns:
            str: The name of the queue.
        """

        raise NotImplementedError()  # pragma: no cover

    @property
    def target(self: "Endpoint") -> Callable[..., Any]:
        """
        Property that returns the target function associated with this endpoint.

        Returns:
            Callable[..., Any]: The target function.
        """

        return self._target

    def __call__(self, /) -> NoReturn:
        raise TypeError(
            f"{self.target.__name__}() should not be called directly. Use App.run() instead"
        )

    def _make_codec(self: "Endpoint", codec_type: CodecType) -> Codec:
        if codec_type not in _codec_type_to_class:  # pragma: no cover
            raise NotImplementedError(f"Unsupported codec type: {codec_type}")

        return _codec_type_to_class[codec_type]()

    def _make_serializer(
        self: "Endpoint", func: Callable, codec_type: CodecType
    ) -> Serializer:
        codec: Codec = self._make_codec(codec_type)

        signature: inspect.Signature = inspect.signature(func)
        if len(signature.parameters) != 2:
            raise FunctionSignatureError(
                f"Function {func.__name__}() must accept exactly two parameters"
            )

        # check for no type annotations. if this is the case, infer no return value
        if func.__annotations__ == {}:
            return self._make_serializer_typeless(function=func, codec=codec)

        # check for a return type annotation
        if "return" in func.__annotations__ and len(func.__annotations__) == 1:
            return self._make_serializer_return_type(function=func, codec=codec)

        raise FunctionSignatureError(
            "Unable to infer serializer type from function annotations for function "
            f"{func.__name__}()"
        )  # pragma: no cover

    def _make_serializer_return_type(
        self: "Endpoint",
        function: Callable,
        codec: Codec,
    ) -> Serializer:
        return ReturnTypeSerializer(function=function, codec=codec)

    def _make_serializer_typeless(
        self: "Endpoint",
        function: Callable,
        codec: Codec,
    ) -> Serializer:
        return TypelessSerializer(function=function, codec=codec)

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
