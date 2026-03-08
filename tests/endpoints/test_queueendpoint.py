from typing import Optional

from mqkit import Response
from mqkit.endpoints import QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.errors import NoForwardTargetError
from mqkit.messaging import Attributes, Forward, Queue, QueueMessage

import pytest

from mqkit.messaging.destination import Destination
from mqkit.messaging.retry.noretrystrategy import NoRetryStrategy


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
            retry_strategy=NoRetryStrategy(),
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
                is_dead_letter=False,
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
            retry_strategy=NoRetryStrategy(),
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
                    is_dead_letter=False,
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
            retry_strategy=NoRetryStrategy(),
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
                is_dead_letter=False,
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
            retry_strategy=NoRetryStrategy(),
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
                is_dead_letter=False,
            ),
        )
    )
    assert result is not None
    assert result.forward_target.name == "response-topic"
    assert result.message.data == b'{"response": "ok"}'


def test_queue_endpoint_with_forward_and_response_topic_error() -> None:
    def target(message, attributes):
        return Response(
            {"response": "ok"},
            topic="specific-topic",
        )

    endpoint = QueueEndpoint(
        QueueEndpointConfig(
            queue=Queue(
                name="test-queue",
            ),
            target=target,
            codec_type="json",
            forward_to=Destination(
                resource=Queue(
                    name="response-queue",
                ),
                topic="response-topic",
            ),
            retry_strategy=NoRetryStrategy(),
        )
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    with pytest.raises(ValueError):
        endpoint.handle_message(
            QueueMessage(
                data=b'{"key": "value"}',
                attributes=Attributes(
                    headers={},
                    forwarded=False,
                    topic=None,
                    is_dead_letter=False,
                ),
            )
        )
