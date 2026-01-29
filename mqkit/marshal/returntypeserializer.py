"""
module mqkit.marshal.returntypeserializer

Defines the ReturnTypeSerializer class responsible for serializing
and deserializing return types in message queue operations.

This serializer ensures that return values can be correctly transmitted
over the message queue by converting them to and from a suitable format.
It does not enforce any constraints on parameters
"""

import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Tuple,
    get_args,
    get_origin,
    Optional,
    Type,
    Union,
)

from pydantic import BaseModel

from mqkit.messaging.response import Response

from ..errors import FunctionSignatureError, SerializeError
from ..marshal.codecs import Codec
from .serializer import Serializer


class ReturnTypeSerializer(Serializer):
    """
    class ReturnTypeSerializer

    Responsible for serializing and deserializing return types in message queue operations.
    This serializer ensures that return values can be correctly transmitted over the message
    queue by converting them to and from a suitable format. It does not enforce any constraints
    on parameters.
    """

    _return_type: Union[None, Type[BaseModel], Type[Dict[str, Any]]]

    def __init__(
        self: "ReturnTypeSerializer", function: Callable, codec: Codec
    ) -> None:
        """
        Initialize the ReturnTypeSerializer with the given function and codec.

        Args:
            function (Callable): The function whose return type is to be serialized.
            codec (Codec): The codec used for serialization and deserialization.

        Returns:
            None
        """

        super().__init__(function, codec)

        # check the validity of the function annotations
        signature: inspect.Signature = inspect.signature(function)
        if "return" not in function.__annotations__:
            raise FunctionSignatureError("Function must have a return type annotation")
        if len(signature.parameters) != 2:
            raise FunctionSignatureError("Function must have exactly two parameters")

        # check that the return type annotation is valid
        self._return_type = signature.return_annotation
        if (
            self._return_type is not None
            and self._return_type is not dict
            and get_origin(signature.return_annotation) is not dict
            and (
                isinstance(signature.return_annotation, type)
                and not issubclass(signature.return_annotation, BaseModel)
            )
        ):
            raise FunctionSignatureError(
                "Return type annotation must be a subclass of BaseModel or Dict"
            )

        self._return_func = self._get_return_func_for_type(
            self._return_type,
            signature=signature,
        )

    def _get_return_func_for_type(
        self: "ReturnTypeSerializer",
        return_type: Any,
        signature: inspect.Signature,
    ) -> Callable[[Any], Any]:
        if return_type is None:
            return self._serialize_none
        if isinstance(signature.return_annotation, type) and issubclass(
            return_type, Response
        ):
            return self._get_return_func_for_response_type(return_type)
        if isinstance(signature.return_annotation, type) and issubclass(
            return_type, BaseModel
        ):
            return self._serialize_base_model
        if return_type is dict or get_origin(return_type) is dict:
            return self._serialize_dict

        raise FunctionSignatureError(
            "Return type annotation must be a subclass of BaseModel, Dict, Response, or None"
        )  # pragma: no cover

    def _get_return_func_for_response_type(
        self: "ReturnTypeSerializer",
        return_type: Any,
    ) -> Callable[[Any], Any]:
        type_args: Tuple = (
            return_type.__pydantic_generic_metadata__["args"]
            if hasattr(return_type, "__pydantic_generic_metadata__")
            and "args" in return_type.__pydantic_generic_metadata__
            else tuple()
        )
        if len(type_args) != 1:
            raise FunctionSignatureError(
                "Response return type annotation must have exactly one content type"
            )

        response_content_type: Type = type_args[0]
        if not issubclass(response_content_type, BaseModel):
            raise FunctionSignatureError(
                "Response return type annotation must have a BaseModel content type"
            )

        # extract the BaseModel type from the Response generic
        self._return_type = response_content_type
        return self._serialize_base_model

    @property
    def return_type(self: "ReturnTypeSerializer") -> Any:
        """
        Property that returns the return type annotation of the function.

        Returns:
            Any: The return type annotation.
        """

        return self._return_type

    def _serialize_none(self: "ReturnTypeSerializer", data: Any) -> None:
        if data is not None:
            raise SerializeError(
                "Function return type is None, but returned value is not None "
                f"(type {type(data).__name__})"
            )

    def _serialize_base_model(
        self: "ReturnTypeSerializer", data: Any
    ) -> Dict[str, Any]:
        if not isinstance(data, self._return_type):  # type: ignore
            raise SerializeError(
                "Function return type is "
                f"{self._return_type.__name__}, but returned value is of type "  # type: ignore
                f"{type(data).__name__}"
            )

        return data.model_dump()

    def _serialize_dict(self: "ReturnTypeSerializer", data: Any) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise SerializeError(
                "Function return type is Dict, but returned value is of type "
                f"{type(data).__name__}"
            )

        return data

    def deserialize(self: "Serializer", data: bytes) -> Any:
        return self._codec.decode(data)

    def serialize(self: "ReturnTypeSerializer", data: Any) -> Optional[bytes]:
        converted_data: Optional[Dict[str, Any]] = self._return_func(data)
        if converted_data is None:
            return None

        return self._codec.encode(converted_data)
