from threading import Thread
import time
from typing import Optional

from mqkit.endpoints import QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.engines import create_engine
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.errors import FunctionTypeError, WorkerTerminatedError
from mqkit.messaging import Queue
from mqkit.messaging.retry.noretrystrategy import NoRetryStrategy
from mqkit.workers.threaded import ThreadCoordinator

import pytest

from ..common import (
    ASSERT_TIMEOUT,
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
                    QueueEndpointConfig(
                        queue=Queue(
                            name="some_queue",
                        ),
                        target=async_function,
                        codec_type="json",
                        retry_strategy=NoRetryStrategy(),
                    )
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
                    QueueEndpointConfig(
                        queue=Queue(
                            name=managed_queue.name,
                        ),
                        target=sync_function,
                        codec_type="json",
                        retry_strategy=NoRetryStrategy(),
                    )
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
        wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
        for i in range(5):
            managed_queue.publish(f'{{"test": "message {i}"}}')
        time.sleep(2.0)

        # interrupt the coordinator and ensure it stops processing messages
        wait_to_assert(lambda: counter == 5, timeout=ASSERT_TIMEOUT)
        coordinator._interrupt(exception=KeyboardInterrupt())
        coordinator_thread.join(timeout=ASSERT_TIMEOUT)
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
                    QueueEndpointConfig(
                        queue=Queue(
                            name=managed_queue.name,
                        ),
                        target=sync_function,
                        codec_type="json",
                        retry_strategy=NoRetryStrategy(),
                    )
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
        wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
        for i in range(10):
            managed_queue.publish(f'{{"test": "message {i}"}}')

        # check that all messages were processed and stop the coordinator
        wait_to_assert(lambda: counter == 10, timeout=ASSERT_TIMEOUT)
        coordinator._interrupt(
            exception=KeyboardInterrupt(),
        )
        coordinator_thread.join(timeout=ASSERT_TIMEOUT)
        assert not coordinator_thread.is_alive()

        # publish some more messages to ensure coordinator is stopped
        for i in range(10, 15):
            managed_queue.publish(f'{{"test": "message {i}"}}')
        time.sleep(ASSERT_TIMEOUT)
        wait_to_assert(lambda: counter == 10, timeout=ASSERT_TIMEOUT)


def test_threadcoordinator_worker_crash_no_recovery(
    rabbitmq_engine: RabbitMqEngine,
    mocker,
) -> None:
    def crashing_run(self):
        time.sleep(2)  # simulate AMQP connection alive for a bit
        raise ConnectionError("Simulated AMQP disconnect")

    mocker.patch(
        "mqkit.workers.threaded.ThreadWorker._process_messages",
        crashing_run,
    )

    coordinator_exception: Optional[Exception] = None

    def capture_coordinator_exception() -> None:
        nonlocal coordinator_exception
        try:
            coordinator.run()
        except Exception as e:
            coordinator_exception = e

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
                    QueueEndpointConfig(
                        queue=Queue(
                            name=managed_queue.name,
                        ),
                        target=sync_function,
                        codec_type="json",
                        retry_strategy=NoRetryStrategy(),
                    )
                )
            ],
            engine=rabbitmq_engine,
        )

        coordinator_thread: Thread = Thread(
            target=capture_coordinator_exception,
            daemon=True,
        )
        coordinator_thread.start()

        wait_to_assert(
            lambda: (
                not (coordinator_thread.is_alive() or coordinator_exception is None)
            ),
            timeout=ASSERT_TIMEOUT,
        )
        assert isinstance(coordinator_exception, WorkerTerminatedError)
        assert isinstance(coordinator_exception.__cause__, ConnectionError)


def test_threadcoordinator_worker_crash_with_recovery(
    rabbitmq_engine: RabbitMqEngine,
    mocker,
) -> None:
    thread_start_counter: int = 0
    crashing_run_stopped: bool = False

    def crashing_run(self):
        nonlocal thread_start_counter
        thread_start_counter += 1

        if thread_start_counter < 3:
            time.sleep(2)  # simulate AMQP connection alive for a bit
            raise ConnectionError("Simulated AMQP disconnect")

        while not crashing_run_stopped:
            time.sleep(1.0)

    mocker.patch(
        "mqkit.workers.threaded.ThreadWorker._process_messages",
        crashing_run,
    )

    coordinator_exception: Optional[Exception] = None

    def capture_coordinator_exception() -> None:
        nonlocal coordinator_exception
        try:
            coordinator.run()
        except Exception as e:
            coordinator_exception = e

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
                    QueueEndpointConfig(
                        queue=Queue(
                            name=managed_queue.name,
                        ),
                        target=sync_function,
                        codec_type="json",
                        retry_strategy=NoRetryStrategy(),
                    )
                )
            ],
            engine=rabbitmq_engine,
            allow_restart=True,
        )

        coordinator_thread: Thread = Thread(
            target=capture_coordinator_exception,
            daemon=True,
        )
        coordinator_thread.start()

        wait_to_assert(
            lambda: thread_start_counter >= 2,
            timeout=ASSERT_TIMEOUT,
        )
