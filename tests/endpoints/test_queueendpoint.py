from typing import Optional

from mqkit.endpoints import QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.errors import NoForwardTargetError
from mqkit.messaging import Attributes, Forward, Queue, QueueMessage

import pytest


def test_queue_endpoint_no_forward_no_result() -> None:
    def target(message, attributes):
        pass

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test-queue",
            ),
            target=target,
            codec_type="json",
        )
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    result: Optional[Forward] = endpoint.handle_message(
        QueueMessage(
            data=b'{"key": "value"}',
            attributes=Attributes(
                headers={},
                forwarded=False,
                topic=None,
            ),
        )
    )
    assert result is None


def test_queue_endpoint_no_forward_with_result() -> None:
    def target(message, attributes):
        return {"response": "ok"}

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test-queue",
            ),
            target=target,
            codec_type="json",
        )
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    with pytest.raises(NoForwardTargetError):
        endpoint.handle_message(
            QueueMessage(
                data=b'{"key": "value"}',
                attributes=Attributes(
                    headers={},
                    forwarded=False,
                    topic=None,
                ),
            )
        )


def test_queue_endpoint_with_forward_queue() -> None:
    def target(message, attributes):
        return {"response": "ok"}

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test-queue",
            ),
            target=target,
            codec_type="json",
            forward_to=Queue(
                name="response-queue",
            ),
        )
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    result: Optional[Forward] = endpoint.handle_message(
        QueueMessage(
            data=b'{"key": "value"}',
            attributes=Attributes(
                headers={},
                forwarded=False,
                topic=None,
            ),
        )
    )
    assert result is not None
    assert isinstance(result.forward_target, Queue)
    assert result.forward_target.name == "response-queue"
    assert result.message.data == b'{"response": "ok"}'


def test_queue_endpoint_with_forward_topic() -> None:
    def target(message, attributes):
        return {"response": "ok"}

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test-queue",
            ),
            target=target,
            codec_type="json",
            forward_to="response-topic",
        )
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    result: Optional[Forward] = endpoint.handle_message(
        QueueMessage(
            data=b'{"key": "value"}',
            attributes=Attributes(
                headers={},
                forwarded=False,
                topic=None,
            ),
        )
    )
    assert result is not None
    assert result.forward_target.name == "response-topic"
    assert result.message.data == b'{"response": "ok"}'
