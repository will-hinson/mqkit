import json
from typing import Any, Dict

import pytest
from pydantic import BaseModel

from mqkit.errors import FunctionSignatureError
from mqkit.marshal import FullyTypedSerializer
from mqkit.marshal.codecs import Codec, JsonCodec
from mqkit.messaging import Attributes


def test_fullytypedserializer_valid_signatures() -> None:
    class MessageModel(BaseModel):
        text: str
        number: int

    def dict_function_no_return(message: Dict, attributes: Attributes) -> None: ...
    def dict_function_with_return(message: Dict, attributes: Attributes) -> Dict: ...
    def model_function_no_return(
        message: MessageModel, attributes: Attributes
    ) -> None: ...
    def model_function_with_return(
        message: MessageModel, attributes: Attributes
    ) -> MessageModel: ...

    for func in (
        dict_function_no_return,
        dict_function_with_return,
        model_function_no_return,
        model_function_with_return,
    ):
        FullyTypedSerializer(func, codec=JsonCodec())


def test_fullytypedserializer_invalid_signatures() -> None:
    def invalid_function_missing_attributes(message: Dict) -> None: ...
    def invalid_function_bad_parameter_name(msg: Dict, attrs: Attributes) -> None: ...
    def invalid_function_bad_message_type(
        message: str, attributes: Attributes
    ) -> None: ...
    def invalid_function_bad_attributes_type(
        message: Dict, attributes: str
    ) -> None: ...

    for func in (
        invalid_function_missing_attributes,
        invalid_function_bad_parameter_name,
        invalid_function_bad_message_type,
        invalid_function_bad_attributes_type,
    ):
        with pytest.raises(FunctionSignatureError):
            FullyTypedSerializer(func, codec=JsonCodec())


def test_fullytypedserializer_deserialization() -> None:
    class MessageModel(BaseModel):
        text: str
        number: int

    def dict_function_with_return(message: Dict, attributes: Attributes) -> Dict:
        return {}

    def model_function_with_return(
        message: MessageModel, attributes: Attributes
    ) -> MessageModel:
        return MessageModel(text=message.text.upper(), number=message.number + 1)

    serializer = FullyTypedSerializer(model_function_with_return, codec=JsonCodec())
    raw_message = b'{"text": "hello", "number": 42}'
    assert serializer.deserialize(raw_message) == MessageModel(text="hello", number=42)

    serializer = FullyTypedSerializer(dict_function_with_return, codec=JsonCodec())
    raw_message = b'{"key": "value", "number": 123}'
    assert serializer.deserialize(raw_message) == {"key": "value", "number": 123}


def test_fullytypedserializer_bad_codec() -> None:
    class MessageModel(BaseModel):
        text: str
        number: int

    def dict_function_with_return(message: Dict, attributes: Attributes) -> Dict:
        return {}

    def model_function_with_return(
        message: MessageModel, attributes: Attributes
    ) -> MessageModel:
        return MessageModel(text=message.text.upper(), number=message.number + 1)

    class BadCodec(Codec):
        @property
        def content_type(self: "Codec") -> str:
            return "application/bad"

        def decode(self: "Codec", data: bytes) -> Any:
            return "invalid type str!"

        def encode(self: "Codec", data: Any) -> bytes:
            raise NotImplementedError()

    serializer = FullyTypedSerializer(model_function_with_return, codec=BadCodec())
    raw_message = b'{"text": "hello", "number": 42}'
    with pytest.raises(TypeError):
        serializer.deserialize(raw_message)

    serializer = FullyTypedSerializer(dict_function_with_return, codec=BadCodec())
    raw_message = b'{"key": "value", "number": 123}'
    with pytest.raises(TypeError):
        serializer.deserialize(raw_message)


def test_fullytypedserializer_model_codec() -> None:
    class MessageModel(BaseModel):
        text: str
        number: int

    def model_function_with_return(
        message: MessageModel, attributes: Attributes
    ) -> MessageModel:
        return MessageModel(text=message.text.upper(), number=message.number + 1)

    class ModelCodec(Codec):
        @property
        def content_type(self: "Codec") -> str:
            return "application/model"

        def decode(self: "Codec", data: bytes) -> MessageModel:
            dict_data = json.loads(data.decode("utf-8"))
            return MessageModel(**dict_data)

        def encode(self: "Codec", data: MessageModel) -> bytes:
            return json.dumps(data.model_dump()).encode("utf-8")

    serializer = FullyTypedSerializer(model_function_with_return, codec=ModelCodec())
    raw_message = b'{"text": "hello", "number": 42}'
    deserialized = serializer.deserialize(raw_message)
    assert isinstance(deserialized, MessageModel)
    assert deserialized == MessageModel(text="hello", number=42)


def test_fullytypedserializer_serialization() -> None:
    class MessageModel(BaseModel):
        text: str
        number: int

    def model_function_with_return(
        message: MessageModel, attributes: Attributes
    ) -> MessageModel:
        return MessageModel(text=message.text.upper(), number=message.number + 1)

    serializer = FullyTypedSerializer(model_function_with_return, codec=JsonCodec())
    message_instance = MessageModel(text="hello", number=42)
    assert json.loads(serializer.serialize(message_instance)) == {  # type: ignore
        "text": "hello",
        "number": 42,
    }
