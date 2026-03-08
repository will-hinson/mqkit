import json
from threading import Thread
from typing import Dict

from mqkit import App, create_engine, ImmediateRetryStrategy
from mqkit.engines.rabbitmq import RabbitMqEngine
import pytest

from mqkit.messaging.attributes import Attributes

from ..common import (
    ASSERT_TIMEOUT,
    TEST_HOST,
    TEST_PASSWORD,
    TEST_PORT,
    TEST_USERNAME,
    TEST_VHOST,
    ManagedQueue,
    wait_to_assert,
)


@pytest.fixture
def rabbitmq_engine() -> RabbitMqEngine:
    return create_engine(
        f"amqp://{TEST_USERNAME}:{TEST_PASSWORD}@{TEST_HOST}:{TEST_PORT}{TEST_VHOST}"
    )  # type: ignore


def test_immediateretrystrategy_basic(rabbitmq_engine: RabbitMqEngine) -> None:
    """Test immediate retry with no dead-letter destination"""
    target_retries: int = 3

    app: App = App()
    with ManagedQueue("test_queue") as managed_queue:
        try_count: int = 0
        last_message: Dict
        last_attributes: Attributes

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=target_retries,
            ),
        )
        def test_queue(message, attributes):
            nonlocal last_message, last_attributes, try_count

            try_count += 1
            last_message = message
            last_attributes = attributes

            raise Exception("Simulated processing failure")

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
            managed_queue.publish("{}")

            # check that the message actually gets retried the expected number of times
            wait_to_assert(lambda: try_count == target_retries + 1, timeout=ASSERT_TIMEOUT)

            # stop the app and check that no more retries happen after the app is stopped
            app.stop()
            with pytest.raises(AssertionError):
                wait_to_assert(
                    lambda: try_count > target_retries + 1,
                    timeout=max(
                        {
                            5,
                            ASSERT_TIMEOUT / 4,
                        }
                    ),
                )

            # check that the queue is also empty after the retries are exhausted
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            assert last_attributes.retry_count == target_retries, (
                "Retry count in attributes should match the number of retries attempted"
            )
            assert (
                len(json.loads(last_attributes.headers["x-mqkit-exception-history"]))
                == target_retries
            ), "Exception history should contain an entry for each retry attempt"
        finally:
            app.stop()
            app_thread.join()


def test_immediateretrystrategy_invalid_retry_count(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    """
    Test that a message in the queue with an invalid retry count in the attributes gets treated
    as having 0 retries so that it gets retried up to the max retries
    """

    with ManagedQueue("test_queue") as managed_queue:
        app: App = App()

        last_attributes: Attributes

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=3,
            ),
        )
        def test_queue(message, attributes):
            nonlocal last_attributes
            last_attributes = attributes

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)

            # publish a message with an invalid retry count in the attributes
            managed_queue.publish(
                "{}",
                headers={
                    "x-mqkit-retry-count": "invalid_retry_count",
                },
            )

            # check that the message gets retried up to the max retries and then stops
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            assert last_attributes.retry_count == 0, (
                "Retry count in attributes should be treated as 0 if it is invalid"
            )
        finally:
            app.stop()
            app_thread.join()
