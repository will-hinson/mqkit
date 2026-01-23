"""
module mqkit.engines.rabbitmqengine

Defines the RabbitMqEngine class for connecting to RabbitMQ message brokers.
"""

from typing import Any, Dict, Type

from pika import PlainCredentials as PikaPlainCredentials
from yarl import URL

from ...connections.amqp import AmqpConnection
from ...credentials import PlainCredentials
from ..engine import Engine
from ...marshal import Queue


class RabbitMqEngine(Engine):
    """
    class RabbitMqEngine

    An engine for connecting to RabbitMQ message brokers using AMQP protocol.
    Provides methods to create AMQP connections to specified queues.
    """

    host: str
    credentials: PlainCredentials
    port: int = 5672
    vhost: str = "/"
    use_amqps: bool = False

    def connect(
        self: "RabbitMqEngine",
        queue: str,
        persistent: bool = True,
        auto_delete: bool = False,
    ) -> AmqpConnection:
        return AmqpConnection(
            host=self.host,
            port=self.port,
            vhost=self.vhost,
            credentials=PikaPlainCredentials(
                self.credentials.username, self.credentials.password
            ),
            queue=Queue(
                name=queue,
                persistent=persistent,
                auto_delete=auto_delete,
            ),
            use_ssl=self.use_amqps,
        )

    @classmethod
    def from_url(cls: Type["RabbitMqEngine"], url: URL) -> "RabbitMqEngine":
        assert url.scheme in ("amqp", "amqps")

        for required_attribute in ("host", "user", "password"):
            if getattr(url, required_attribute) is None:
                raise AttributeError(
                    f"URL is missing required attribute: {required_attribute}"
                )

        ctor_args: Dict[str, Any] = {
            "host": url.host,
            "credentials": PlainCredentials(
                username=url.user,  # type: ignore
                password=url.password,  # type: ignore
            ),
            "use_amqps": url.scheme == "amqps",
        }
        if url.path not in (None, ""):
            ctor_args["vhost"] = url.path
        if url.port is not None:
            ctor_args["port"] = url.port
        elif url.scheme == "amqps":
            ctor_args["port"] = 5671

        return RabbitMqEngine(**ctor_args)
