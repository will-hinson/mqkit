from typing import Optional

from mqkit.endpoints import QueueEndpoint
from mqkit.errors.noforwardtargeterror import NoForwardTargetError
from mqkit.marshal import Forward, QueueMessage

import pytest


def test_queue_endpoint_no_forward_no_result() -> None:
    def target(message, attributes):
        pass

    endpoint = QueueEndpoint(
        queue_name="test-queue",
        target=target,
        codec_type="json",
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    result: Optional[Forward] = endpoint.handle_message(
        QueueMessage(
            data=b'{"key": "value"}',
            attributes={},
        )
    )
    assert result is None


def test_queue_endpoint_no_forward_with_result() -> None:
    def target(message, attributes):
        return {"response": "ok"}

    endpoint = QueueEndpoint(
        queue_name="test-queue",
        target=target,
        codec_type="json",
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    with pytest.raises(NoForwardTargetError):
        endpoint.handle_message(
            QueueMessage(
                data=b'{"key": "value"}',
                attributes={},
            )
        )


def test_queue_endpoint_with_forward_queue() -> None:
    def target(message, attributes):
        return {"response": "ok"}

    endpoint = QueueEndpoint(
        queue_name="test-queue",
        target=target,
        codec_type="json",
        forward_to="response-queue",
    )

    assert endpoint.queue_name == "test-queue"
    assert endpoint.target.__code__ != target.__code__

    result: Optional[Forward] = endpoint.handle_message(
        QueueMessage(
            data=b'{"key": "value"}',
            attributes={},
        )
    )
    assert result is not None
    assert result.forward_target == "response-queue"
    assert result.message.data == b'{"response": "ok"}'
