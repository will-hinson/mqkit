from copy import deepcopy
import queue
from threading import Thread
from typing import Dict, List, Optional

from mqkit import App, Exchange, Queue, create_engine, ImmediateRetryStrategy
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.errors import ConfigurationError
from mqkit.messaging.attributes import Attributes
import pytest

from mqkit.messaging.destination import Destination

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


def stop_app_permissive(app: App) -> None:
    """
    Stop the app, but ignore any exceptions that occur during stopping to ensure that the app is stopped even if there are issues with stopping it
    """
    try:
        app.stop()
    except queue.ShutDown:
        pass


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
            wait_to_assert(
                lambda: try_count == target_retries + 1, timeout=ASSERT_TIMEOUT
            )

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
            assert len(last_attributes.exception_history) == target_retries, (
                "Exception history should contain an entry for each retry attempt"
            )
        finally:
            stop_app_permissive(app)
            app_thread.join()


def test_immediateretrystrategy_marshal_error_no_retry(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    """Test that messages that fail due to a MarshalError do not get retried since they are likely malformed and will continue to fail on retry"""

    app: App = App()
    with ManagedQueue("test_queue") as managed_queue:
        try_count: int = 0

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=3,
            ),
        )
        def test_queue(message, attributes):
            nonlocal try_count
            try_count += 1

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
            managed_queue.publish("this is not a valid json message")

            # check that the message does not get retried and the failure is acknowledged immediately
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: try_count == 0, timeout=ASSERT_TIMEOUT)
        finally:
            stop_app_permissive(app)
            app_thread.join()

        wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)


def test_immediateretrystrategy_negative_retries() -> None:
    """Test that configuring the ImmediateRetryStrategy with a negative number of retries raises a ConfigurationError"""

    with pytest.raises(ConfigurationError):
        ImmediateRetryStrategy(retries=-1)


def test_immediateretrystrategy_zero_retries(rabbitmq_engine: RabbitMqEngine) -> None:
    """Test that if the retry strategy is configured with 0 retries, the message does not get retried at all"""

    app: App = App()
    with ManagedQueue("test_queue") as managed_queue:
        try_count: int = 0

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=0,
            ),
        )
        def test_queue(message, attributes):
            nonlocal try_count
            try_count += 1
            raise Exception("Simulated processing failure")

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
            managed_queue.publish("{}")

            # check that the message does not get retried and the failure is acknowledged immediately
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: try_count == 1, timeout=ASSERT_TIMEOUT)
        finally:
            stop_app_permissive(app)
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
            stop_app_permissive(app)
            app_thread.join()


def test_immediateretrystrategy_invalid_exception_history(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    """
    Test that a message in the queue with an invalid exception history in the attributes gets treated
    as having an empty exception history so that the retry strategy can still append to the history
    and retry the message up to the max retries
    """

    with ManagedQueue("test_queue") as managed_queue:
        app: App = App()

        first_attributes: Attributes
        attributes_list: List[Attributes] = []

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=3,
            ),
        )
        def test_queue(message, attributes):
            nonlocal attributes_list, first_attributes
            attributes_list.append(attributes)

            if len(attributes_list) == 1:
                first_attributes = deepcopy(attributes)

            raise Exception("Simulated processing failure")

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)

            # publish a message with an invalid exception history in the attributes
            managed_queue.publish(
                "{}",
                headers={
                    "x-mqkit-exception-history": "invalid_exception_history",
                },
            )
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: len(attributes_list) == 4, timeout=ASSERT_TIMEOUT)

            assert len(first_attributes.exception_history) == 0, (
                "Exception history should be treated as empty if it is invalid"
            )
        finally:
            stop_app_permissive(app)
            app_thread.join()


def test_immediateretrystrategy_dlq(
    rabbitmq_engine: RabbitMqEngine,
) -> None:
    """Test that messages that exhaust their retries get forwarded to the dead-letter destination if configured"""

    app: App = App()
    with ManagedQueue("test_queue") as managed_queue, ManagedQueue("dlq") as dlq:
        managed_queue.define()
        dlq.define()

        try_count: int = 0

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=3,
                dead_letter_destination=Destination(
                    resource=Queue(name=dlq.name),
                    topic="test_topic",
                ),
            ),
        )
        def test_queue(message, attributes):
            nonlocal try_count
            try_count += 1
            raise Exception("Simulated processing failure")

        dlq_message: Optional[Dict] = None
        dlq_attributes: Optional[Attributes] = None

        @app.queue(dlq.name)
        def dlq_queue(message, attributes):
            nonlocal dlq_attributes, dlq_message
            dlq_message = message
            dlq_attributes = attributes

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: dlq.exists, timeout=ASSERT_TIMEOUT)
            managed_queue.publish("{}")

            # check that the message gets retried up to the max retries and then forwarded to the dlq
            wait_to_assert(
                lambda: try_count == 4, timeout=ASSERT_TIMEOUT
            )  # initial try + 3 retries
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: dlq_message is not None, timeout=ASSERT_TIMEOUT)

            # inspect the received message in the dlq
            assert dlq_attributes is not None
            assert dlq_message == {}
            assert dlq_attributes.exception_history, (
                "Exception history should be present in the attributes of the message "
                "forwarded to the dlq"
            )
            assert dlq_attributes.forwarded, (
                "Message should be marked as forwarded in the attributes when forwarded to the dlq"
            )
            assert dlq_attributes.is_dead_letter, (
                "Message should be marked as a dead letter in the attributes when forwarded "
                "to the dlq"
            )
            assert dlq_attributes.topic == "test_topic", (
                "Topic attribute should be preserved when forwarding to the dlq"
            )
            assert dlq_attributes.retry_count == 0, (
                "Retry count should be reset to 0 when forwarding to the dlq"
            )
            assert dlq_attributes.previous_retry_count == 3, (
                "Previous retry count header should reflect the number of retries attempted before"
                "forwarding to the dlq"
            )

            # look at the exception history in the attributes and check that it contains an entry
            # for each retry attempt with the correct exception information
            for i in range(3):
                exception_history_entry = dlq_attributes.exception_history[i]
                assert exception_history_entry.exception_type == "Exception", (
                    "Exception type in the history should match the type of exception raised "
                    "in the handler"
                )
                assert (
                    exception_history_entry.exception_message
                    == "Simulated processing failure"
                ), (
                    "Exception message in the history should match the message of the exception "
                    "raised in the handler"
                )
                assert exception_history_entry.retry_count == i, (
                    "Retry count in each history entry should reflect the retry attempt number"
                )
                assert len(exception_history_entry.traceback) > 0
                assert exception_history_entry.exception_module == "builtins", (
                    "Exception module in the history should match the module of the exception "
                    "raised in the handler"
                )
        finally:
            stop_app_permissive(app)
            app_thread.join()


def test_immediateretrystrategy_dl_exchange(rabbitmq_engine: RabbitMqEngine) -> None:
    """
    Test that messages that exhaust their retries get forwarded to the dead-letter
    destination exchange if configured
    """

    app: App = App()

    with ManagedQueue("test_queue") as managed_queue, ManagedQueue("dlq") as dlq:
        managed_queue.define()
        dlq.define()

        app.declare(
            Exchange(
                name="dl_exchange",
                type="fanout",
            )
        ).bind(
            Queue(
                name=dlq.name,
            ),
            topic="",
        )

        try_count: int = 0

        @app.queue(
            managed_queue.name,
            retry_strategy=ImmediateRetryStrategy(
                retries=3,
                dead_letter_destination=Destination(
                    resource=Exchange(
                        name="dl_exchange",
                    ),
                    topic="test_topic",
                ),
            ),
        )
        def test_queue(message, attributes):
            nonlocal try_count
            try_count += 1
            raise Exception("Simulated processing failure")

        dlq_message: Optional[Dict] = None
        dlq_attributes: Optional[Attributes] = None

        @app.queue(dlq.name)
        def dlq_queue(message, attributes):
            nonlocal dlq_attributes, dlq_message
            dlq_message = message
            dlq_attributes = attributes

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        try:
            wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: dlq.exists, timeout=ASSERT_TIMEOUT)
            managed_queue.publish("{}")

            # check that the message gets retried up to the max retries and then forwarded to the dlq
            wait_to_assert(
                lambda: try_count == 4, timeout=ASSERT_TIMEOUT
            )  # initial try + 3 retries
            wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
            wait_to_assert(lambda: dlq_message is not None, timeout=ASSERT_TIMEOUT)
            assert dlq_message == {}
            assert dlq_attributes is not None
            assert dlq_attributes.forwarded, (
                "Message should be marked as forwarded in the attributes when forwarded to the dlq"
            )
            assert dlq_attributes.is_dead_letter, (
                "Message should be marked as a dead letter in the attributes when forwarded "
                "to the dlq"
            )
            assert dlq_attributes.topic == "test_topic", (
                "Topic attribute should be preserved when forwarding to the dlq"
            )
            assert dlq_attributes.platform["method"]["exchange"] == "dl_exchange", (
                "Message should be forwarded to the configured dl_exchange exchange"
            )
        finally:
            stop_app_permissive(app)
            app_thread.join()
