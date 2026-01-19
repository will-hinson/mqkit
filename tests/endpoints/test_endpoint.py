import inspect

from mqkit.endpoints import Endpoint, QueueEndpoint
from mqkit.errors import EndpointSignatureError
from mqkit.marshal.codecs import CodecType

import pytest


def test_endpoint_is_abstract_base_class() -> None:
    for function in [
        "handle_message",
    ]:
        assert hasattr(Endpoint, function)
        assert getattr(Endpoint, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Endpoint, function)(
                *(
                    [None]
                    * len(inspect.signature(getattr(Endpoint, function)).parameters)
                )
            )


def test_endpoint_cannot_call() -> None:
    endpoint = QueueEndpoint(
        queue_name="test",
        target=lambda a, b: None,
        codec_type=CodecType.JSON,
    )

    with pytest.raises(TypeError):
        endpoint()


def test_endpoint_bad_signature() -> None:
    with pytest.raises(EndpointSignatureError):
        QueueEndpoint(
            queue_name="test",
            target=lambda a: None,
            codec_type=CodecType.JSON,
        )
