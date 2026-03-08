import os
import time
from typing import Callable, Set, Type
import uuid

import requests
from requests import Response
from slugify import slugify

ASSERT_TIMEOUT: float = 30.0

TEST_HOST: str = os.environ.get("RABBITMQ_HOST", "localhost")
TEST_PORT: int = int(os.environ.get("RABBITMQ_PORT", "5672"))
TEST_USERNAME: str = os.environ.get("RABBITMQ_USER", "admin")
TEST_PASSWORD: str = os.environ.get("RABBITMQ_PASSWORD", "123456")
TEST_VHOST: str = os.environ.get("RABBITMQ_VHOST", "/")

TEST_MANAGEMENT_PORT: int = int(os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672"))


class ManagedQueue:
    """
    Context manager for creating and deleting a managed RabbitMQ queue for testing purposes.
    """

    base_queue_name: str
    name: str
    uuid: str

    def __init__(self: "ManagedQueue", base_queue_name: str) -> None:
        self.base_queue_name = base_queue_name

        self.uuid = str(uuid.uuid4())
        self.name = f"{self.base_queue_name}_{self.uuid}"

    def __enter__(self: "ManagedQueue") -> "ManagedQueue":
        # ensure the queue does not already exist
        response: Response = requests.delete(
            build_management_url(f"/api/queues/%2F/{self.name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        assert response.ok or response.status_code == 404, (
            "Failed to delete pre-existing queue"
        )

        return self

    def __exit__(self: "ManagedQueue", exc_type, exc_value, traceback) -> None:
        # delete the queue and resubmit exchange when we're done
        requests.delete(
            build_management_url(f"/api/queues/%2F/{self.name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )
        requests.delete(
            build_management_url(f"/api/exchanges/%2F/{self.resubmit_exchange}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )

    def define(self: "ManagedQueue") -> None:
        response: Response = requests.put(
            build_management_url(f"/api/queues/%2F/{self.name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "auto_delete": False,
                "durable": True,
                "arguments": {},
            },
        )
        assert response.ok, "Failed to define the queue"

    @property
    def exists(self: "ManagedQueue") -> int:
        response: Response = requests.get(
            build_management_url(f"/api/queues/%2F/{self.name}"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
        )

        assert response.ok or response.status_code == 404
        return "messages" in response.json()

    def publish(
        self: "ManagedQueue", message: str, headers: dict | None = None
    ) -> None:
        response: Response = requests.post(
            build_management_url("/api/exchanges/%2F/amq.default/publish"),
            auth=(TEST_USERNAME, TEST_PASSWORD),
            json={
                "properties": {"headers": headers or {}},
                "routing_key": self.name,
                "payload": message,
                "payload_encoding": "string",
            },
        )
        assert response.ok, "Failed to publish message to the queue"
        assert response.json().get("routed", False), "Message was not routed to a queue"

    @property
    def resubmit_exchange(self: "ManagedQueue") -> str:
        return f"mqkit.resubmit.{slugify(self.name, separator='_')}"

    @property
    def size(self: "ManagedQueue") -> int:
        data = {}
        while data.get("messages") is None:
            response: Response = requests.get(
                build_management_url(f"/api/queues/%2F/{self.name}"),
                auth=(TEST_USERNAME, TEST_PASSWORD),
            )
            assert response.ok, (
                f"Failed to get queue information for {self.name}: {response}"
            )
            data = response.json()

        return data["messages"]


def build_management_url(endpoint: str) -> str:
    return f"http://{TEST_HOST}:{TEST_MANAGEMENT_PORT}{endpoint}"


def wait_to_assert(
    assertion_func: Callable,
    timeout: float = 5.0,
    interval: float = 0.1,
    allow: Set[Type[Exception]] = set(),
) -> None:
    start_time = time.time()
    while True:
        try:
            assert assertion_func()
            return
        except Exception as e:
            if type(e) not in allow and type(e) is not AssertionError:
                raise
            if time.time() - start_time > timeout:
                raise
            time.sleep(interval)
