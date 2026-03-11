"""
module mqkit.marshal.fullytypedserializer

Defines the FullyTypedSerializer for serializing function inputs and outputs with
full type information.
"""

import inspect
from typing import Any, Callable, Dict, get_origin, Optional, Type, override

from pydantic import BaseModel

from .codecs.codec import Codec
from ..errors import ConfigurationError, FunctionSignatureError
from ..messaging import Attributes
from .returntypeserializer import ReturnTypeSerializer


class FullyTypedSerializer(ReturnTypeSerializer):
    """
    class FullyTypedSerializer

    Serializer that enforces full type annotations on function parameters and return type.
    """

    _message_type: Optional[Type[BaseModel]] = None
    _codec_return_type: Type

    def __init__(
        self: "FullyTypedSerializer",
        function: Callable,
        codec: Codec,
    ) -> None:
        # call the ReturnTypeSerializer constructor to check the return type
        super().__init__(
            function=function,
            codec=codec,
        )

        if "return" not in self._codec.decode.__annotations__:
            raise ConfigurationError(
                f"Codec {type(self._codec).__name__} cannot be used with FullyTypedSerializer "
                "as its decode method lacks a return type annotation"
            )

        codec_type_origin = get_origin(self._codec.decode.__annotations__["return"])
        self._codec_return_type = (
            codec_type_origin
            if isinstance(codec_type_origin, type)
            else self._codec.decode.__annotations__["return"]
        )
        self._check_function_signature(function)

    @override
    def deserialize(self: "FullyTypedSerializer", data: bytes) -> Any:
        result: Any = self._codec.decode(data)

        if self._message_type is not None:
            if isinstance(result, self._message_type):
                return result
            if isinstance(result, Dict):
                return self._message_type(**result)

            raise TypeError(
                f"Deserialized data is of type {type(result).__name__}, "
                f"expected {self._message_type.__name__} or Dict"
            )

        if isinstance(result, self._codec_return_type):
            return result

        raise TypeError(
            f"Codec returned value of type {type(result).__name__}, "
            f"expected {self._codec_return_type.__name__} or model"
        )

    @override
    def serialize(self: "FullyTypedSerializer", data: Any) -> Optional[bytes]:
        # since we inherit from ReturnTypeSerializer, we can just use its serialize logic
        return super().serialize(data)

    def _check_function_signature(
        self: "FullyTypedSerializer",
        function: Callable,
    ) -> None:
        signature: inspect.Signature = inspect.signature(function)

        self._check_type_annotations(function, signature)

    def _check_type_annotations(
        self: "FullyTypedSerializer",
        function: Callable,
        signature: inspect.Signature,
    ) -> None:
        if len(signature.parameters) != 2:  # pragma: no cover
            raise FunctionSignatureError(
                f"Function {function.__name__}() must accept exactly two parameters"
            )

        for ann_name in (
            "message",
            "attributes",
            "return",
        ):
            if ann_name not in function.__annotations__:
                raise FunctionSignatureError(
                    f"Function {function.__name__}() must have type annotation for '{ann_name}'"
                )

        # check the message parameter type and populate the message model type
        self._populate_message_model_type(
            function,
            signature.parameters["message"].annotation,
        )

        if signature.parameters["attributes"].annotation is not Attributes:
            raise FunctionSignatureError(
                f"Function {function.__name__}() 'attributes' parameter type "
                f"must be mqkit.messaging.Attributes"
            )

    def _populate_message_model_type(
        self: "FullyTypedSerializer",
        function: Callable,
        annotation: Type,
    ) -> None:
        annotation_origin: Type = get_origin(annotation) or annotation

        # check if the message parameter is annotated as the return type of
        # the codec's decode() method (i.e., Dict for JsonCodec or bytes for RawCodec)
        #
        # if so, this is valid but we don't need to decode a model type
        if issubclass(annotation_origin, self._codec_return_type) and not issubclass(
            annotation_origin, BaseModel
        ):
            return

        if not issubclass(annotation_origin, BaseModel):
            raise FunctionSignatureError(
                f"Function {function.__name__}() 'message' parameter type "
                f"must be a subclass of pydantic.BaseModel or correspond to "
                f"the {self._codec_return_type.__name__} return type annotation of "
                f"{type(self._codec).__name__}.decode(). Current annotation type is "
                f"{annotation.__name__}"
            )

        self._message_type = annotation
