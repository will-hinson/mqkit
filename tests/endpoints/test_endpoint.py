import inspect

from pydantic import BaseModel

from mqkit.endpoints import Endpoint, QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.errors import FunctionSignatureError
from mqkit.marshal import ReturnTypeSerializer, TypelessSerializer
from mqkit.marshal.codecs import CodecType
from mqkit.messaging import Queue
from mqkit.messaging.retry import NoRetryStrategy, RetryStrategy

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
        QueueEndpointConfig(
            queue=Queue(
                name="test",
            ),
            target=lambda a, b: None,
            codec_type=CodecType.JSON,
            retry_strategy=NoRetryStrategy(),
        )
    )

    with pytest.raises(TypeError):
        endpoint()


def test_endpoint_bad_signature() -> None:
    with pytest.raises(FunctionSignatureError):
        QueueEndpoint(
            QueueEndpointConfig(
                queue=Queue(
                    name="test",
                ),
                target=lambda a: None,
                codec_type=CodecType.JSON,
                retry_strategy=NoRetryStrategy(),
            )
        )


def test_endpoint_type_detection() -> None:
    def typeless_handler(a, b):
        pass

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test",
            ),
            target=typeless_handler,
            codec_type=CodecType.JSON,
            retry_strategy=NoRetryStrategy(),
        )
    )
    assert isinstance(
        {
            name: cell.cell_contents
            for name, cell in zip(
                endpoint.target.__code__.co_freevars,
                endpoint.target.__closure__,  # type: ignore
            )
        }["serializer"],
        TypelessSerializer,
    )

    class TestModel(BaseModel):
        x: int
        y: str

    def return_type_handler_type1(a, b) -> dict:
        return {}

    def return_type_handler_type2(a, b) -> None:
        return None

    def return_type_handler_type3(a, b) -> TestModel:
        return TestModel(x=1, y="test")

    for return_type_handler in [
        return_type_handler_type1,
        return_type_handler_type2,
        return_type_handler_type3,
    ]:
        endpoint = QueueEndpoint(
            QueueEndpointConfig(
                queue=Queue(
                    name="test",
                ),
                target=return_type_handler,
                codec_type=CodecType.JSON,
                retry_strategy=NoRetryStrategy(),
            )
        )
        assert isinstance(
            {
                name: cell.cell_contents
                for name, cell in zip(
                    endpoint.target.__code__.co_freevars,
                    endpoint.target.__closure__,  # type: ignore
                )
            }["serializer"],
            ReturnTypeSerializer,
        )
