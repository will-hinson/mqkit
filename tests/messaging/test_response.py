import pytest
from mqkit.messaging import Response


def test_response_data_property() -> None:
    response = Response(content={"key": "value"})

    # Initially, accessing data should raise ValueError
    with pytest.raises(ValueError):
        response.data
    assert not response.has_data

    # Set the serialized data
    serialized_bytes = b'{"key": "value"}'
    response.data = serialized_bytes

    # Now accessing data should return the set value
    assert response.has_data
    assert response.data == serialized_bytes

    # Setting data to non-bytes should not be allowed
    with pytest.raises(TypeError):
        response.data = None
    with pytest.raises(TypeError):
        response.data = "not bytes"  # type: ignore
