import time

from mqkit import create_engine, NoRetry
from mqkit.endpoints import QueueEndpoint
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.messaging import Queue
from mqkit.workers.threaded import ThreadWorker

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


def test_threadworker_no_forwarding(rabbitmq_engine: RabbitMqEngine) -> None:
    with ManagedQueue("test_queue") as managed_queue:
        # define a simple target function that counts messages
        counter: int = 0

        def target(message, attributes):
            nonlocal counter
            counter += 1

        # define/start the ThreadWorker and wait for the queue to be ready
        worker: ThreadWorker = ThreadWorker(
            QueueEndpoint(
                QueueEndpointConfig(
                    queue=Queue(
                        name=managed_queue.name,
                    ),
                    target=target,
                    codec_type="json",
                )
            ),
            engine=rabbitmq_engine,
        )
        worker.start()

        try:
            # wait for the queue to be ready
            wait_to_assert(lambda: managed_queue.exists, timeout=5.0)

            # publish some test messages and check that they are processed
            for n in range(5):
                managed_queue.publish(f'{{"msg": {n}}}')
            wait_to_assert(lambda: counter == 5, timeout=10.0)
        finally:
            worker.stop("Test complete")
            worker.join()
            assert worker.stopped

        # now, put a few more messages in and ensure they are not processed
        for n in range(5, 10):
            managed_queue.publish(f'{{"msg": {n}}}')
        time.sleep(5.0)
        wait_to_assert(lambda: counter == 5, timeout=5.0)


def test_threadworker_no_retry(rabbitmq_engine: RabbitMqEngine) -> None:
    with ManagedQueue("test_queue_no_retry") as managed_queue:
        # define a simple target function that always raises NoRetry
        counter: int = 0

        def target(message, attributes):
            nonlocal counter
            counter += 1
            raise NoRetry("Intentional failure")

        # define/start the ThreadWorker and wait for the queue to be ready
        worker: ThreadWorker = ThreadWorker(
            QueueEndpoint(
                QueueEndpointConfig(
                    queue=Queue(
                        name=managed_queue.name,
                    ),
                    target=target,
                    codec_type="json",
                )
            ),
            engine=rabbitmq_engine,
        )
        worker.start()

        try:
            # wait for the queue to be ready
            wait_to_assert(lambda: managed_queue.exists, timeout=5.0)

            # publish some test messages and check that they are processed
            for n in range(5):
                managed_queue.publish(f'{{"msg": {n}}}')
            wait_to_assert(lambda: counter == 5, timeout=10.0)

            # ensure that the messages are not reprocessed
            time.sleep(5.0)
            wait_to_assert(lambda: counter == 5, timeout=5.0)
        finally:
            worker.stop("Test complete")
            worker.join()
            assert worker.stopped


def test_threadworker_with_queue_forwarding(rabbitmq_engine: RabbitMqEngine) -> None:
    with (
        ManagedQueue("source_queue") as source_queue,
        ManagedQueue("forwarded_queue") as forwarded_queue,
    ):
        # define a simple target function that counts messages
        counter: int = 0

        def target(message, attributes):
            nonlocal counter
            counter += 1

            return {"forward": True}

        # define/start the ThreadWorker and wait for the queue to be ready
        worker: ThreadWorker = ThreadWorker(
            QueueEndpoint(
                QueueEndpointConfig(
                    queue=Queue(
                        name=source_queue.name,
                    ),
                    target=target,
                    codec_type="json",
                    forward_to=forwarded_queue.name,
                )
            ),
            engine=rabbitmq_engine,
        )
        worker.start()

        try:
            # wait for the queue to be ready
            wait_to_assert(lambda: source_queue.exists, timeout=5.0)

            # publish some test messages and check that they are processed
            for n in range(5):
                source_queue.publish(f'{{"msg": {n}}}')
            wait_to_assert(lambda: counter == 5, timeout=10.0)

            # inspect the status of the forwarded queue
            wait_to_assert(lambda: forwarded_queue.size == 5, timeout=5.0)
        finally:
            worker.stop("Test complete")
            worker.join()
            assert worker.stopped

        # now, put a few more messages in and ensure they are not processed
        for n in range(5, 10):
            source_queue.publish(f'{{"msg": {n}}}')
        time.sleep(5.0)
        wait_to_assert(lambda: counter == 5, timeout=5.0)
        wait_to_assert(lambda: forwarded_queue.size == 5, timeout=5.0)
