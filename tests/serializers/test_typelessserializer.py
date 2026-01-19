from typing import Optional

from mqkit.errors import SerializeError
from mqkit.marshal import TypelessSerializer
from mqkit.marshal.codecs import JsonCodec

import pytest


def test_typelessserializer_deserialize() -> None:
    serializer = TypelessSerializer(JsonCodec())

    data: Optional[object] = serializer.deserialize(b'{"key": "value", "number": 42}')
    assert isinstance(data, dict)
    assert data == {"key": "value", "number": 42}


def test_typelessserializer_serialize() -> None:
    serializer = TypelessSerializer(JsonCodec())

    assert serializer.serialize(None) is None

    serialized: Optional[bytes] = serializer.serialize({"key": "value", "number": 42})
    assert isinstance(serialized, bytes)

    with pytest.raises(SerializeError):
        serializer.serialize(set([1, 2, 3]))  # type: ignore
