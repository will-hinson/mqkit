from threading import Thread
import time

from mqkit.endpoints import QueueEndpoint
from mqkit.engines import create_engine
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.errors import FunctionTypeError
from mqkit.workers.threaded import ThreadCoordinator

import pytest

from ..common import (
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


def test_threadcoordinator_bad_endpoint_function(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    async def async_function(message, attributes):
        return {"response": "This is an async function"}

    with pytest.raises(FunctionTypeError):
        ThreadCoordinator(
            endpoints=[
                QueueEndpoint(
                    "some_queue",
                    target=async_function,
                    codec_type="json",
                )
            ],
            engine=rabbitmq_engine,
        )


def test_threadcoordinator_keyboard_interrupt(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    with ManagedQueue("some_queue") as managed_queue:
        counter: int = 0

        def sync_function(message, attributes):
            nonlocal counter
            counter += 1

        # start a coordinator on a separate thread so we can inspect queue
        # state while it's running
        coordinator = ThreadCoordinator(
            endpoints=[
                QueueEndpoint(
                    managed_queue.name,
                    target=sync_function,
                    codec_type="json",
                )
            ],
            engine=rabbitmq_engine,
        )
        coordinator_thread: Thread = Thread(
            target=coordinator.run,
            daemon=True,
        )
        coordinator_thread.start()

        # publish some messages to the queue
        wait_to_assert(lambda: managed_queue.exists, timeout=5.0)
        for i in range(5):
            managed_queue.publish(f'{{"test": "message {i}"}}')
        time.sleep(2.0)

        # interrupt the coordinator and ensure it stops processing messages
        wait_to_assert(lambda: counter == 5, timeout=5.0)
        coordinator._interrupt(exception=KeyboardInterrupt())
        coordinator_thread.join(timeout=5.0)
        assert not coordinator_thread.is_alive()

        # publish some more messages to ensure coordinator is stopped
        for worker in coordinator._workers:  # type: ignore
            worker.join()
            assert not worker.is_alive()
        for i in range(5, 10):
            managed_queue.publish(f'{{"test": "message {i}"}}')
        assert counter == 5


def test_threadcoordinator_single_endpoint(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    with ManagedQueue("some_queue") as managed_queue:
        counter: int = 0

        def sync_function(message, attributes):
            nonlocal counter
            counter += 1

        # start a coordinator on a separate thread so we can inspect queue
        # state while it's running
        coordinator = ThreadCoordinator(
            endpoints=[
                QueueEndpoint(
                    managed_queue.name,
                    target=sync_function,
                    codec_type="json",
                )
            ],
            engine=rabbitmq_engine,
        )
        coordinator_thread: Thread = Thread(
            target=coordinator.run,
            daemon=True,
        )
        coordinator_thread.start()

        # wait for the queue to be ready and publish some messages
        wait_to_assert(lambda: managed_queue.exists, timeout=5.0)
        for i in range(10):
            managed_queue.publish(f'{{"test": "message {i}"}}')

        # check that all messages were processed and stop the coordinator
        wait_to_assert(lambda: counter == 10, timeout=5.0)
        coordinator._stop_workers(
            exception=KeyboardInterrupt(),
            reason="Test complete",
        )
        coordinator_thread.join(timeout=5.0)
        assert not coordinator_thread.is_alive()

        # publish some more messages to ensure coordinator is stopped
        for i in range(10, 15):
            managed_queue.publish(f'{{"test": "message {i}"}}')
        time.sleep(5.0)
        wait_to_assert(lambda: counter == 10, timeout=2.0)
