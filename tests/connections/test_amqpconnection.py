from mqkit import create_engine
from mqkit.connections.amqp import AmqpConnection
from mqkit.engines import RabbitMqEngine
from mqkit.errors import ShutdownRequested
from mqkit.marshal import Forward, Queue, QueueMessage

import pytest
import requests

from ..common import (
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
        wait_to_assert(lambda: managed_queue.size == 1, timeout=5.0)

        # get the message and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data
        assert message.attributes.platform is not None
        assert not message.attributes.platform["method"].get("redelivered", True)

        # acknowledge the message as successfully processed and verify the queue is empty
        connection.acknowledge_success(message)
        wait_to_assert(lambda: managed_queue.size == 0, timeout=5.0)


def test_amqp_connection_failure(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue(base_queue_name="test_queue") as managed_queue,
        rabbitmq_engine.connect(queue=managed_queue.name) as connection,
    ):
        message_data: str = f'{{"test":"message","uuid":"{managed_queue.uuid}"}}'

        # publish the test message and wait for it to arrive
        assert managed_queue.size == 0
        managed_queue.publish(message_data)
        wait_to_assert(lambda: managed_queue.size == 1, timeout=5.0)

        # get the message and verify its contents
        message: QueueMessage = connection.get_message()
        assert message.data.decode("utf-8") == message_data
        assert message.attributes.platform is not None
        assert not message.attributes.platform["method"].get("redelivered", True)

        # acknowledge the message as failed. by default, it won't be requeued
        connection.acknowledge_failure(message)
        wait_to_assert(lambda: managed_queue.size == 1, timeout=5.0)


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
        wait_to_assert(lambda: source_queue.size == 1, timeout=5.0)

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
        wait_to_assert(lambda: source_queue.size == 0, timeout=5.0)
        wait_to_assert(lambda: target_queue.size == 1, timeout=5.0)


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


def test_amqp_connection_ssl() -> None:
    ssl_engine = create_engine(
        f"amqps://{TEST_USERNAME}:{TEST_PASSWORD}@{TEST_HOST}:{TEST_PORT}{TEST_VHOST}"
    )  # type: ignore

    with pytest.raises(RuntimeError):
        with ssl_engine.connect(queue="my_ssl_queue") as _:
            ...
