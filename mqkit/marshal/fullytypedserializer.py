"""
module mqkit.marshal.fullytypedserializer

Defines the FullyTypedSerializer for serializing function inputs and outputs with
full type information.
"""

import inspect
from typing import Any, Callable, Dict, Optional, Type, override

from pydantic import BaseModel

from .codecs.codec import Codec
from ..errors import FunctionSignatureError
from ..messaging import Attributes
from .returntypeserializer import ReturnTypeSerializer


class FullyTypedSerializer(ReturnTypeSerializer):
    """
    class FullyTypedSerializer

    Serializer that enforces full type annotations on function parameters and return type.
    """

    _message_type: Optional[Type[BaseModel]] = None

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

        if isinstance(result, Dict):
            return result

        raise TypeError(
            f"Codec returned value of type {type(result).__name__}, expected Dict or model"
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
        if issubclass(annotation, Dict):
            return

        if not issubclass(annotation, BaseModel):
            raise FunctionSignatureError(
                f"Function {function.__name__}() 'message' parameter type "
                f"must be a subclass of pydantic.BaseModel"
            )

        self._message_type = annotation
