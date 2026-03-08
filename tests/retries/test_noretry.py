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
    build_management_url,
    wait_to_assert,
)


@pytest.fixture
def rabbitmq_engine() -> RabbitMqEngine:
    return create_engine(
        f"amqp://{TEST_USERNAME}:{TEST_PASSWORD}@{TEST_HOST}:{TEST_PORT}{TEST_VHOST}"
    )  # type: ignore


def test_noretry(rabbitmq_engine: RabbitMqEngine, mocker) -> None:
    app: App = App()
    with ManagedQueue("noretry") as managed_queue:
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
