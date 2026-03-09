"""
module mqkit.endpoints.endpoint

Abstract base class for message queue endpoints.
"""

from abc import ABCMeta, abstractmethod
import functools
import inspect
from typing import Any, Callable, Dict, NoReturn, Optional, Type, TypeAlias, Union

from mqkit.messaging.attributes import Attributes
from pydantic import ValidationError

from ..errors import FunctionSignatureError, MarshalError
from ..marshal import (
    FullyTypedSerializer,
    ReturnTypeSerializer,
    Serializer,
    TypelessSerializer,
)
from ..marshal.codecs import (
    Codec,
    CodecType,
    JsonCodec,
    MessagePackCodec,
    RawCodec,
    YamlCodec,
)
from ..messaging import Destination, Forward, QueueMessage, Response
from ..messaging.retry import RetryStrategy

_codec_type_to_class: Dict[CodecType, Type[Codec]] = {
    CodecType.JSON: JsonCodec,
    CodecType.MESSAGEPACK: MessagePackCodec,
    CodecType.RAW: RawCodec,
    CodecType.YAML: YamlCodec,
}

EndpointCallback: TypeAlias = Callable[
    [Any, Attributes], Optional[Union[Response, bytes]]
]
EndpointExceptionHandler: TypeAlias = Callable[[bytes, Attributes, Exception], None]
EndpointDecodeException: TypeAlias = Union[MarshalError, ValidationError]


class Endpoint(metaclass=ABCMeta):
    """
    class Endpoint

    Abstract base class for message queue endpoints.
    """

    _is_async: bool = False
    _target: Callable[..., Optional[Response]]

    def __init__(self: "Endpoint", target: Callable, codec_type: CodecType) -> None:
        self._is_async = inspect.iscoroutinefunction(target)
        self._target = self._wrap_with_decode(
            target,
            codec_type=codec_type,
        )

    @property
    @abstractmethod
    def dead_letter(self: "Endpoint") -> Optional[Destination]:  # pragma: no cover
        """
        Property that returns the dead letter destination for this endpoint, if any.

        Returns:
            Optional[Destination]: The dead letter destination, or None if not configured.
        """

        raise NotImplementedError(
            f"Endpoint of type {type(self).__name__} does not implement dead_letter()"
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
    @abstractmethod
    def is_auto_delete(self: "Endpoint") -> bool:  # pragma: no cover
        """
        Property that indicates whether the endpoint is auto-delete.

        Returns:
            bool: True if the endpoint is auto-delete, False otherwise.
        """

        raise NotImplementedError(
            f"Endpoint of type {type(self).__name__} does not implement is_auto_delete()"
        )

    @property
    @abstractmethod
    def is_persistent(self: "Endpoint") -> bool:  # pragma: no cover
        """
        Property that indicates whether the endpoint is persistent.

        Returns:
            bool: True if the endpoint is persistent, False otherwise.
        """

        raise NotImplementedError(
            f"Endpoint of type {type(self).__name__} does not implement is_persistent()"
        )

    def make_forward_headers(
        self: "Endpoint", response: Response, origin_queue: str
    ) -> Dict[str, str]:
        """
        Creates headers for forwarding a response message.

        Args:
            response (Response): The response message to create headers for.

        Returns:
            Dict[str, str]: A dictionary of headers for the forwarded message.
        """

        return {
            "x-mqkit-forwarded": "true",
            "x-mqkit-origin-queue": origin_queue,
        } | response.headers

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
    @abstractmethod
    def retry_strategy(self: "Endpoint") -> RetryStrategy:
        """
        Property that returns the retry strategy associated with this endpoint.

        Returns:
            RetryStrategy: The retry strategy for the endpoint.
        """

        raise NotImplementedError()  # pragma: no cover

    @property
    def target(self: "Endpoint") -> Callable[..., Optional[Response]]:
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
        self: "Endpoint", func: EndpointCallback, codec_type: CodecType
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

        # just try constructing a fully-typed serializer. it will raise an appropriate
        # FunctionSignatureError if it can't make sense of the signature
        return self._make_serializer_fully_typed(function=func, codec=codec)

    def _make_serializer_fully_typed(
        self: "Endpoint",
        function: Callable,
        codec: Codec,
    ) -> Serializer:
        return FullyTypedSerializer(function=function, codec=codec)

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
        self: "Endpoint",
        func: EndpointCallback,
        codec_type: CodecType,
    ) -> Callable[..., Optional[Response]]:
        serializer: Serializer = self._make_serializer(
            func=func,
            codec_type=codec_type,
        )

        @functools.wraps(func)
        def _deserialize_wrapper(
            *,
            message: bytes,
            attributes: Attributes,
        ) -> Optional[Response]:
            # call the function and get the result. it could be a Response object, some
            # data, or None. we want to marshal this into Response | None
            result: Optional[Union[Response, bytes]] = func(
                serializer.deserialize(message),
                attributes,
            )
            if not isinstance(result, Response):
                result = Response(content=result)

            # serialize the content into byte data and return the Response. if the serializer
            # returns None, Response.data will raise an error
            serialized_data: Optional[bytes] = serializer.serialize(result.content)
            if serialized_data is None:
                return None
            result.data = serialized_data
            return result

        return _deserialize_wrapper
