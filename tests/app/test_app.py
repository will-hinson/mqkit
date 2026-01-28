from threading import Thread
from typing import Set
import uuid

from mqkit import App, Queue, create_engine
from mqkit.declarations.queuedeclaration import QueueDeclaration
from mqkit.engines.rabbitmq import RabbitMqEngine
from mqkit.errors import FunctionTypeError

import pytest
import requests

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


def test_app_cannot_declare_after_start(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()
    assert not app.started

    app_thread: Thread = Thread(target=app.run, args=(rabbitmq_engine,))
    app_thread.start()

    with pytest.raises(RuntimeError):

        @app.queue("new_queue")
        def new_queue(message, attributes):
            return {"response": "Message processed by new_queue"}

    with pytest.raises(RuntimeError):

        @app.on_start
        def on_start():
            pass

    with pytest.raises(RuntimeError):

        @app.on_shutdown
        def on_shutdown():
            pass

    with pytest.raises(RuntimeError):
        app.exchange("new_exchange", type="fanout")

    assert app.started
    app.stop()
    app_thread.join()


def test_app_declare_exchange(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()

    for exchange_name in ["test_exchange", "another_test_exchange"]:
        response = requests.delete(
            build_management_url("/api/exchanges/%2F/test_exchange"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok or response.status_code == 404

    with ManagedQueue("test_queue") as mq1, ManagedQueue("another_test_queue") as mq2:
        mq1.define()
        mq2.define()
        wait_to_assert(
            lambda: mq1.exists and mq2.exists,
            timeout=ASSERT_TIMEOUT,
        )

        app.exchange("another_test_exchange", type="direct")

        app.exchange(
            "test_exchange",
            type="fanout",
        ).bind_queue(
            mq1.name,
            topic="test.topic",
        ).bind_queue(
            mq2.name,
            topic="another.test.topic",
        ).bind_exchange(
            "another_test_exchange",
            topic="exchange.topic",
        )

        app_thread: Thread = Thread(
            target=app.run,
            args=(rabbitmq_engine,),
            daemon=True,
        )
        app_thread.start()

        for exchange_name in ["test_exchange", "another_test_exchange"]:
            wait_to_assert(
                lambda: requests.get(
                    build_management_url(f"/api/exchanges/%2F/{exchange_name}"),
                    auth=(TEST_USERNAME, TEST_PASSWORD),
                ).ok,
                timeout=ASSERT_TIMEOUT,
            )

        response_json = requests.get(
            build_management_url("/api/exchanges/%2F/test_exchange"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        ).json()
        assert response_json["name"] == "test_exchange"
        assert response_json["type"] == "fanout"

        response_json = requests.get(
            build_management_url("/api/exchanges/%2F/test_exchange/bindings/source"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        ).json()
        sources: Set[str] = {binding["destination"] for binding in response_json}
        for resource_name in [mq1.name, mq2.name, "another_test_exchange"]:
            assert resource_name in sources

        app.stop()

        for exchange_name in ["test_exchange", "another_test_exchange"]:
            response = requests.delete(
                build_management_url(f"/api/exchanges/%2F/{exchange_name}"),
                auth=(TEST_USERNAME, TEST_PASSWORD),
            )
            assert response.ok


def test_app_declare_bad_type() -> None:
    app: App = App()

    with pytest.raises(TypeError):
        app.declare(resource=123456)  # type: ignore


def test_app_declare_queue(rabbitmq_engine: RabbitMqEngine) -> None:
    app: App = App()

    exchange_id: str = str(uuid.uuid4())
    queue_id: str = str(uuid.uuid4())

    exchange_declaration_1 = app.exchange(
        name=exchange_id + "_1",
        type="fanout",
    )
    exchange_declaration_2 = app.exchange(
        name=exchange_id + "_2",
        type="fanout",
    )
    queue_declaration: QueueDeclaration = app.declare(
        Queue(
            name=queue_id,
            persistent=True,
        ),
    )  # type: ignore
    queue_declaration_2: QueueDeclaration = app.declare(
        Queue(
            name=queue_id + "_2",
            persistent=True,
        ),
    )  # type: ignore
    app.exchange(
        exchange_id,
        type="fanout",
    ).bind_queue(
        queue_declaration,
        topic="test.topic",
    ).bind_exchange(
        exchange_declaration_1,
        topic="exchange.topic",
    ).bind(
        exchange_declaration_2,
        topic="another.exchange.topic",
    ).bind(
        queue_declaration_2,
        topic="another.test.topic",
    )

    app_thread: Thread = Thread(
        target=app.run,
        args=(rabbitmq_engine,),
        daemon=True,
    )
    app_thread.start()

    for resource_name in [
        exchange_id,
        exchange_declaration_1.exchange.name,
        exchange_declaration_2.exchange.name,
    ]:
        wait_to_assert(
            lambda: requests.get(
                build_management_url(f"/api/exchanges/%2F/{resource_name}"),
                auth=(TEST_USERNAME, TEST_PASSWORD),
            ).ok,
            timeout=ASSERT_TIMEOUT,
        )
    for resource_name in [queue_id, queue_id + "_2"]:
        wait_to_assert(
            lambda: requests.get(
                build_management_url(f"/api/queues/%2F/{resource_name}"),
                auth=(TEST_USERNAME, TEST_PASSWORD),
            ).ok,
            timeout=ASSERT_TIMEOUT,
        )

    for resource_name in [queue_id, queue_id + "_2"]:
        response = requests.delete(
            build_management_url(f"/api/queues/%2F/{resource_name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok
    for resource_name in [
        exchange_id,
        exchange_declaration_1.exchange.name,
        exchange_declaration_2.exchange.name,
    ]:
        response = requests.delete(
            build_management_url(f"/api/exchanges/%2F/{resource_name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok
