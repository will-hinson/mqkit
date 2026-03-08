import json
import uuid

from requests.exceptions import JSONDecodeError
from slugify import slugify
from mqkit import create_engine
from mqkit.connections.amqp import AmqpConnection
from mqkit.endpoints import QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.engines import RabbitMqEngine
from mqkit.errors import ShutdownRequested
from mqkit.messaging import Exchange, Forward, Queue, QueueMessage
from mqkit.messaging.retry import NoRetryStrategy

import pytest
import requests

from mqkit.messaging.destination import Destination
from mqkit.messaging.response import Response

from ..common import (
    ASSERT_TIMEOUT,
    build_management_url,
    ManagedQueue,
    TEST_PASSWORD,
    TEST_USERNAME,
    TEST_PORT,
    TEST_HOST,
    TEST_VHOST,
    wait_to_assert,
)


@pytest.fixture
def rabbitmq_engine() -> RabbitMqEngine:
    return create_engine(
        f"amqp://{TEST_USERNAME}:{TEST_PASSWORD}@{TEST_HOST}:{TEST_PORT}{TEST_VHOST}"
    )  # type: ignore


def test_amqp_connection_init(rabbitmq_engine: RabbitMqEngine) -> None:
    with rabbitmq_engine.connect(queue="my_queue") as connection:
        assert isinstance(connection, AmqpConnection)

        # check connection parameters
        assert connection.host == TEST_HOST
        assert connection.port == TEST_PORT

        assert any(
            map(
                lambda queue_spec: queue_spec["name"] == "my_queue",
                requests.get(
                    build_management_url("/api/queues"),
                    auth=(TEST_USERNAME, TEST_PASSWORD),
                ).json(),
            )
        )

    response = requests.delete(
        build_management_url("/api/queues/%2F/my_queue"),
        auth=(TEST_USERNAME, TEST_PASSWORD),
    )
    assert response.ok
    response = requests.delete(
        build_management_url("/api/exchanges/%2F/mqkit.resubmit.my_queue"),
        auth=(TEST_USERNAME, TEST_PASSWORD),
    )
    assert response.ok


def test_amqp_connection_not_connected(rabbitmq_engine: RabbitMqEngine) -> None:
    connection: AmqpConnection = rabbitmq_engine.connect(queue="my_queue")
    assert connection._connection is None and connection._channel is None

    for target_func in [
        connection.acknowledge_success,
        connection.acknowledge_failure,
        connection.forward_message,
        lambda _: connection._start_consuming(),
        connection.unblock,
    ]:
        with pytest.raises(RuntimeError):
            target_func(None)  # type: ignore


def test_amqp_connection_success(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue(base_queue_name="test_queue") as managed_queue,
        rabbitmq_engine.connect(queue=managed_queue.name) as connection,
    ):
        message_data: str = f'{{"test":"message","uuid":"{managed_queue.uuid}"}}'

        # publish the test message and wait for it to arrive
        assert managed_queue.size == 0
        managed_queue.publish(message_data)
        wait_to_assert(lambda: managed_queue.size == 1, timeout=ASSERT_TIMEOUT)

        # get the message and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data
        assert message.attributes.platform is not None
        assert not message.attributes.platform["method"].get("redelivered", True)

        # acknowledge the message as successfully processed and verify the queue is empty
        connection.acknowledge_success(message)
        wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)


def test_amqp_connection_failure(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue(base_queue_name="test_queue") as managed_queue,
        rabbitmq_engine.connect(queue=managed_queue.name) as connection,
    ):
        message_data: str = f'{{"test":"message","uuid":"{managed_queue.uuid}"}}'

        # publish the test message and wait for it to arrive
        assert managed_queue.size == 0
        managed_queue.publish(message_data)
        wait_to_assert(lambda: managed_queue.size == 1, timeout=ASSERT_TIMEOUT)

        # get the message and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data
        assert message.attributes.platform is not None
        assert not message.attributes.platform["method"].get("redelivered", True)

        # acknowledge the message as failed. by default, it won't be requeued
        connection.acknowledge_failure(message)
        wait_to_assert(lambda: managed_queue.size == 1, timeout=ASSERT_TIMEOUT)


def test_amqp_connection_forwarding_exchange(rabbitmq_engine: RabbitMqEngine) -> None:
    try:
        # delete the unmanaged exchange and its bindings if they exist
        response = requests.delete(
            build_management_url("/api/queues/%2F/unmanaged_queue"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok or response.status_code == 404
        response = requests.delete(
            build_management_url("/api/exchanges/%2F/unmanaged_exchange"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok or response.status_code == 404

        # create an unmanaged exchange and bound queue to forward messages to
        response = requests.put(
            build_management_url("/api/exchanges/%2F/unmanaged_exchange"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "type": "fanout",
                "durable": True,
                "auto_delete": False,
            },
        )
        assert response.ok
        response = requests.put(
            build_management_url("/api/queues/%2F/unmanaged_queue"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "durable": True,
                "auto_delete": False,
                "exclusive": False,
            },
        )
        assert response.ok
        response = requests.post(
            build_management_url(
                "/api/bindings/%2F/e/unmanaged_exchange/q/unmanaged_queue"
            ),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={"routing_key": "", "arguments": {}},
        )
        assert response.ok

        with (
            ManagedQueue(base_queue_name="source_queue") as source_queue,
            rabbitmq_engine.connect(queue=source_queue.name) as connection,
        ):
            message_data: str = f'{{"test":"message","uuid":"{source_queue.uuid}"}}'

            # publish the test message to the source queue and wait for it to arrive
            assert source_queue.size == 0
            source_queue.publish(message_data)
            wait_to_assert(lambda: source_queue.size == 1, timeout=ASSERT_TIMEOUT)

            # get the message from the source queue and verify its contents
            message: QueueMessage = connection.get_message()
            assert message.data.decode("utf-8") == message_data

            # forward the message to the target exchange
            connection.forward_message(
                Forward(
                    forward_target=Exchange(
                        name="unmanaged_exchange",
                    ),
                    message=message,
                )
            )

            # acknowledge the original message as successfully processed
            connection.acknowledge_success(message)

            # verify the source queue is empty and the target queue has the forwarded message
            wait_to_assert(lambda: source_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(
                lambda: (
                    requests.get(
                        build_management_url("/api/queues/%2F/unmanaged_queue"),
                        auth=(TEST_USERNAME, TEST_PASSWORD),
                    ).json()["messages"]
                    == 1
                ),
                timeout=ASSERT_TIMEOUT,
                allow={JSONDecodeError},
            )
    finally:
        # clean up the unmanaged exchange and its bindings
        response = requests.delete(
            build_management_url("/api/queues/%2F/unmanaged_queue"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok
        response = requests.delete(
            build_management_url("/api/exchanges/%2F/unmanaged_exchange"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok


def test_amqp_connection_forwarding_queue(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue(base_queue_name="source_queue") as source_queue,
        ManagedQueue(base_queue_name="target_queue") as target_queue,
        rabbitmq_engine.connect(queue=source_queue.name) as connection,
        rabbitmq_engine.connect(queue=target_queue.name) as _,
    ):
        message_data: str = f'{{"test":"message","uuid":"{source_queue.uuid}"}}'

        # publish the test message to the source queue and wait for it to arrive
        assert source_queue.size == 0
        assert target_queue.size == 0
        source_queue.publish(message_data)
        wait_to_assert(lambda: source_queue.size == 1, timeout=ASSERT_TIMEOUT)

        # get the message from the source queue and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data

        # forward the message to the target queue
        connection.forward_message(
            forward=Forward(
                forward_target=Queue(
                    name=target_queue.name,
                ),
                message=message,
            )
        )

        # acknowledge the original message as successfully processed
        connection.acknowledge_success(message)

        # verify the source queue is empty and the target queue has the forwarded message
        wait_to_assert(lambda: source_queue.size == 0, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: target_queue.size == 1, timeout=ASSERT_TIMEOUT)


def test_amqp_connection_shutdown(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue(base_queue_name="test_queue") as managed_queue,
        rabbitmq_engine.connect(queue=managed_queue.name) as connection,
    ):
        # submit a sentinel message to unblock the connection
        connection.unblock(message="forced sentinel")

        with pytest.raises(ShutdownRequested):
            connection.get_message()

        assert connection._message_queue.is_shutdown


def test_amqp_connection_forwarding_with_headers(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    with (
        ManagedQueue(base_queue_name="source_queue") as source_queue,
        rabbitmq_engine.connect(queue=source_queue.name) as connection,
    ):
        message_data: str = f'{{"test":"message","uuid":"{source_queue.uuid}"}}'

        # publish the test message to the source queue and wait for it to arrive
        assert source_queue.size == 0
        source_queue.publish(message_data)
        wait_to_assert(lambda: source_queue.size == 1, timeout=ASSERT_TIMEOUT)

        # get the message from the source queue and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data

        # forward the message to the target queue
        endpoint: QueueEndpoint = QueueEndpoint(
            config=QueueEndpointConfig(
                queue=Queue(name="source_queue"),
                codec_type="json",
                target=lambda x, y: x,
                forward_to="target_queue",
                retry_strategy=NoRetryStrategy(),
            )
        )
        response = Response(
            content={"forwarded": True},
            headers={"X-Custom-Header": "CustomValue"},
        )
        with pytest.raises(ValueError):
            # make sure forwarding fails if no data is set
            endpoint._forward_result(response)
        response.data = b'{"forwarded": true}'
        forward = endpoint._forward_result(response)
        assert forward is not None

        # verify that the forwarded message has the correct headers
        assert forward.message.attributes.headers == {
            "x-mqkit-forwarded": "true",
            "x-mqkit-origin-queue": "source_queue",
            "X-Custom-Header": "CustomValue",
        }


def test_amqp_connection_ssl() -> None:
    ssl_engine = create_engine(
        f"amqps://{TEST_USERNAME}:{TEST_PASSWORD}@{TEST_HOST}:{TEST_PORT}{TEST_VHOST}"
    )  # type: ignore

    with pytest.raises(RuntimeError):
        with ssl_engine.connect(queue="my_ssl_queue") as _:
            ...


def test_amqp_connection_forwarding_with_topic(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    with (
        ManagedQueue(base_queue_name="source_queue") as source_queue,
        rabbitmq_engine.connect(queue=source_queue.name) as connection,
    ):
        target_queue_name: str = f"target_queue_{uuid.uuid4()}"
        response = requests.put(
            build_management_url(f"/api/queues/%2F/{target_queue_name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "durable": True,
                "auto_delete": False,
                "exclusive": False,
            },
        )
        assert response.ok

        message_data: str = f'{{"test":"message","uuid":"{source_queue.uuid}"}}'

        # publish the test message to the source queue and wait for it to arrive
        assert source_queue.size == 0
        source_queue.publish(message_data)
        wait_to_assert(lambda: source_queue.size == 1, timeout=ASSERT_TIMEOUT)

        # get the message from the source queue and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data

        # forward the message to the target queue
        endpoint: QueueEndpoint = QueueEndpoint(
            config=QueueEndpointConfig(
                queue=Queue(name="source_queue"),
                codec_type="json",
                target=lambda x, y: x,
                forward_to=Destination(
                    resource=Queue(name=target_queue_name),
                    topic="my.topic",
                ),
                retry_strategy=NoRetryStrategy(),
            )
        )
        response = Response(
            content={"forwarded": True},
            headers={"X-Custom-Header": "CustomValue"},
        )
        with pytest.raises(ValueError):
            # make sure forwarding fails if no data is set
            endpoint._forward_result(response)
        response.data = b'{"forwarded": true}'
        forward = endpoint._forward_result(response)
        assert forward is not None

        # verify that the forwarded message has the correct headers
        assert forward.message.attributes.headers == {
            "x-mqkit-forwarded": "true",
            "x-mqkit-origin-queue": "source_queue",
            "X-Custom-Header": "CustomValue",
        }
        assert forward.message.attributes.topic == "my.topic"

        # forward the message using the connection then retrieve it from the target queue
        connection.forward_message(forward)
        wait_to_assert(
            lambda: (
                requests.get(
                    build_management_url(f"/api/queues/%2F/{target_queue_name}"),
                    auth=(TEST_USERNAME, TEST_PASSWORD),
                ).json()["messages"]
                == 1
            ),
            timeout=ASSERT_TIMEOUT,
            allow={JSONDecodeError},
        )
        response = requests.post(
            build_management_url(f"/api/queues/%2F/{target_queue_name}/get"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "count": 1,
                "ackmode": "ack_requeue_false",
                "encoding": "auto",
                "truncate": 50000,
            },
        )
        assert response.ok
        message_recv = response.json()[0]
        assert json.loads(message_recv["payload"]) == {"forwarded": True}
        assert message_recv["routing_key"] == "my.topic"
        assert message_recv["properties"]["headers"] == {
            "x-mqkit-forwarded": "true",
            "x-mqkit-origin-queue": "source_queue",
            "X-Custom-Header": "CustomValue",
        }

        # clean up the target queue and exchange
        response = requests.delete(
            build_management_url(f"/api/queues/%2F/{target_queue_name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok
        response = requests.delete(
            build_management_url(
                f"/api/exchanges/%2F/mqkit.resubmit.{slugify(target_queue_name, separator='_')}"
            ),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok
