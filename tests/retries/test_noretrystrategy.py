from threading import Thread

from mqkit import App, create_engine
from mqkit.engines.rabbitmq import RabbitMqEngine
import pytest

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


def test_noretrystrategy_no_forward(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()
    with ManagedQueue("noretrystrategy_no_forward") as managed_queue:
        retry_count: int = 0

        @app.queue(managed_queue.name)
        def test_queue(message, attributes):
            nonlocal retry_count
            retry_count += 1
            raise Exception("Simulated processing failure")

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
        managed_queue.publish("{}")
        wait_to_assert(lambda: retry_count == 1, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: retry_count == 1, timeout=ASSERT_TIMEOUT)
        app.stop()


def test_noretrystrategy_dlq_forward(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()
    with (
        ManagedQueue("noretrystrategy_dlq_forward") as managed_queue,
        ManagedQueue("noretrystrategy_dlq_forward_dlq") as dlq,
    ):
        managed_queue.define()
        dlq.define()

        retry_count: int = 0

        @app.queue(managed_queue.name, dead_letter=dlq.name)
        def test_queue(message, attributes):
            nonlocal retry_count
            retry_count += 1
            raise Exception("Simulated processing failure")

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: dlq.exists, timeout=ASSERT_TIMEOUT)
        managed_queue.publish("{}")
        wait_to_assert(lambda: retry_count == 1, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: managed_queue.size == 0, timeout=ASSERT_TIMEOUT)
        wait_to_assert(lambda: dlq.size == 1, timeout=ASSERT_TIMEOUT)
        assert len(dlq.get_one()["properties"]["headers"]) > 0, (
            "Expected message to have headers added by retry strategy when forwarded to DLQ"
        )
        app.stop()
