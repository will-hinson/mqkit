from threading import Thread

from mqkit import App, create_engine
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.errors import FunctionTypeError

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


def test_app_function_compatibility() -> None:
    app: App = App(concurrency_mode="thread", codec="json")

    with pytest.raises(FunctionTypeError):

        @app.queue("test_queue")
        async def test_queue(message, attributes):
            return {"response": "Message processed by test_queue"}

    with pytest.raises(FunctionTypeError):

        @app.on_start  # type: ignore
        async def on_start():
            pass


def test_app_events(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()

    on_start_called: bool = False
    on_shutdown_called: bool = False

    @app.on_start
    def on_start():
        nonlocal on_start_called
        on_start_called = True

    @app.on_shutdown
    def on_shutdown():
        nonlocal on_shutdown_called
        on_shutdown_called = True

    app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
    app_thread.start()
    app.stop()
    app_thread.join()

    assert on_start_called, "on_start was not called"
    assert on_shutdown_called, "on_shutdown was not called"


def test_app_register_queue(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()

    with ManagedQueue("test_queue") as managed_queue:

        @app.queue(managed_queue.name)
        def test_queue(message, attributes):
            return {"response": "Message processed by test_queue"}

        assert len(app._endpoints) == 1
        assert app._endpoints[0].queue_name == managed_queue.name

        app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
        app_thread.start()
        wait_to_assert(lambda: managed_queue.exists, timeout=ASSERT_TIMEOUT)
        app.stop()


def test_app_start_once(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()

    app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
    app_thread.start()

    with pytest.raises(RuntimeError):
        app.run(engine=rabbitmq_engine)

    app.stop()
    app_thread.join()


def test_app_stop_requires_start() -> None:
    app: App = App()

    with pytest.raises(RuntimeError):
        app.stop()
