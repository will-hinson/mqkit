import json
from typing import Dict, Optional
from pydantic import BaseModel
import pytest
from mqkit.errors import FunctionSignatureError, SerializeError
from mqkit.marshal import ReturnTypeSerializer
from mqkit.marshal.codecs import JsonCodec
from mqkit.messaging import Response


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


def test_returntypeserializer_optional_return_type() -> None:
    from typing import Optional

    def valid_function(message, parameters) -> Optional[Dict]:
        return {"key": "value"}

    def none_function(message, parameters) -> Optional[Dict]:
        return None

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    serialized = serializer.serialize(valid_function(None, None))
    assert serialized is not None
    assert json.loads(serialized) == {"key": "value"}

    serialized_none = serializer.serialize(none_function(None, None))
    assert serialized_none is None


def test_returntypeserializer_response_return_type() -> None:
    class TestModel(BaseModel):
        hello: str

    def valid_function(message, parameters) -> Optional[Response[TestModel]]:
        return Response(content=TestModel(hello="world"))

    def none_function(message, parameters) -> Optional[Response[TestModel]]:
        return None

    serializer: ReturnTypeSerializer = ReturnTypeSerializer(
        function=valid_function, codec=JsonCodec()
    )
    response = valid_function(None, None)
    assert isinstance(response, Response)
    # the endpoint will unwrap the Response and serialize its content. simulate this
    serialized = serializer.serialize(response.content)
    assert serialized is not None
    assert json.loads(serialized) == {"hello": "world"}

    response_none = none_function(None, None)
    assert response_none is None
    serialized_none = serializer.serialize(response_none)
    assert serialized_none is None

    with pytest.raises((TypeError, FunctionSignatureError)):

        def invalid_function(message, parameters) -> Optional[Response[TestModel, int]]:
            return Response(content=123)

        ReturnTypeSerializer(function=invalid_function, codec=JsonCodec())

    with pytest.raises(FunctionSignatureError):

        def invalid_function_2(message, parameters) -> Response[int]:
            return Response(content=123)

        ReturnTypeSerializer(function=invalid_function_2, codec=JsonCodec())
