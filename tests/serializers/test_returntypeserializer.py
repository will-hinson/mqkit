import json
from typing import Dict
from pydantic import BaseModel
import pytest
from mqkit.errors import FunctionSignatureError, SerializeError
from mqkit.marshal import ReturnTypeSerializer
from mqkit.marshal.codecs import JsonCodec


def test_returntypeserializer_deserialize() -> None:
    def sample_function(message, parameters) -> Dict:
        return {"key": "value"}

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=sample_function, codec=JsonCodec()
    )
    assert serializer.deserialize(b'{"key": "value"}') == {"key": "value"}


def test_returntypeserializer_invalid_methods() -> None:
    def missing_return(message, parameters):
        return 123

    def wrong_parameters(message) -> None: ...

    def wrong_return_type(message, attributes) -> str:
        return "invalid"

    for func in wrong_parameters, missing_return, wrong_return_type:
        with pytest.raises(FunctionSignatureError):
            ReturnTypeSerializer(function=func, codec=JsonCodec())


def test_returntypeserializer_return_type_basemodel() -> None:
    class TestModel(BaseModel):
        id: int
        name: str

    def valid_function(message, parameters) -> TestModel:
        return TestModel(id=1, name="Test")

    def invalid_function(message, parameters) -> TestModel:
        return {"id": 1, "name": "Test"}  # type: ignore

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    serialized = serializer.serialize(valid_function(None, None))
    assert serialized is not None
    assert json.loads(serialized) == {"id": 1, "name": "Test"}

    serializer = ReturnTypeSerializer(function=invalid_function, codec=JsonCodec())
    with pytest.raises(SerializeError):
        serializer.serialize(invalid_function(None, None))


def test_returntypeserializer_return_type_dict() -> None:
    def valid_function(message, parameters) -> Dict:
        return {"key": "value"}

    def invalid_function(message, parameters) -> Dict:
        return ["not", "a", "dict"]  # type: ignore

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    serialized = serializer.serialize(valid_function(None, None))
    assert serialized is not None
    assert json.loads(serialized) == {"key": "value"}

    serializer = ReturnTypeSerializer(function=invalid_function, codec=JsonCodec())
    with pytest.raises(SerializeError):
        serializer.serialize(invalid_function(None, None))


def test_returntypeserializer_return_type_none() -> None:
    def valid_function(message, parameters) -> None:
        return

    def invalid_function(message, parameters) -> None:
        return 123  # type: ignore

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    assert serializer.serialize(valid_function(None, None)) is None

    serializer = ReturnTypeSerializer(function=invalid_function, codec=JsonCodec())
    with pytest.raises(SerializeError):
        assert serializer.serialize(invalid_function(None, None)) is None


def test_returntypeserializer_return_type_property() -> None:
    class TestModel(BaseModel):
        id: int
        name: str

        @property
        def info(self) -> str:
            return f"{self.id}-{self.name}"

    def valid_function(message, parameters) -> TestModel:
        return TestModel(id=1, name="Test")

    def dict_function(message, parameters) -> Dict:
        return {"id": 1, "name": "Test"}

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    assert serializer.return_type is TestModel

    serializer = ReturnTypeSerializer(function=dict_function, codec=JsonCodec())
    assert serializer.return_type is Dict
