"""
module mqkit.connections.amqp.amqpconnection

Defines the AmqpConnection class for managing AMQP connections in message
queue applications. This connection class is intended to be returned by
the RabbitMqEngine class
"""

import functools
from queue import Queue
import threading
from typing import ClassVar, Optional, Set

from pika import BasicProperties, ConnectionParameters, PlainCredentials, SSLOptions
from pika import BlockingConnection as PikaBlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method
from pydantic import BaseModel, ConfigDict, PrivateAttr
from slugify import slugify

from .amqpconsumethread import AmqpConsumeThread
from .amqpmessage import AmqpMessage
from .amqpsentinel import AmqpSentinel
from ..connection import Connection
from ...errors import ShutdownRequested
from ...marshal import Forward, QueueMessage


class AmqpConnection(Connection, BaseModel):
    """
    class AmqpConnection

    Manages an AMQP connection using Pika's BlockingConnection. Provides methods
    for acknowledging message processing results, forwarding messages, and
    retrieving messages from the queue
    """

    host: str
    port: int
    vhost: str
    credentials: PlainCredentials
    queue: str
    use_ssl: bool = False

    _channel: Optional[BlockingChannel] = PrivateAttr(default=None)
    _connection: Optional[PikaBlockingConnection] = PrivateAttr(default=None)
    _consume_thread: Optional[AmqpConsumeThread] = PrivateAttr(default=None)
    _declared_queues: Set[str] = PrivateAttr(default_factory=set)
    _message_queue: Queue = PrivateAttr(default_factory=Queue)

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    def acknowledge_failure(
        self: "AmqpConnection",
        message: QueueMessage,
    ) -> None:
        if self._connection is None or self._channel is None:
            raise RuntimeError("AMQP channel is not established")

        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_nack,
                delivery_tag=self._get_delivery_tag(message),
                requeue=False,
            )
        )

    def acknowledge_success(self: "AmqpConnection", message: QueueMessage) -> None:
        if self._connection is None or self._channel is None:
            raise RuntimeError("AMQP channel is not established")

        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_ack,
                delivery_tag=self._get_delivery_tag(message),
            )
        )

    def _declare_queue(
        self: "AmqpConnection",
        queue_name: str,
        *,
        durable: bool = True,
        thread_local: bool = False,
    ) -> None:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")

        # if we've already declared this queue, skip it
        if queue_name in self._declared_queues:  # pragma: no cover
            return

        done_event: threading.Event = threading.Event()

        def declare() -> None:
            if self._channel is None:  # pragma: no cover
                raise RuntimeError("AMQP channel is not established")

            self._channel.queue_declare(  # pyright: ignore[reportOptionalMemberAccess]
                queue=queue_name,
                durable=durable,
            )
            done_event.set()

        if not thread_local:  # pragma: no cover
            self._connection.add_callback_threadsafe(declare)
            done_event.wait()
        else:
            declare()

        self._declared_queues.add(queue_name)

    def _enqueue_message(
        self: "AmqpConnection",
        channel: BlockingChannel,
        method: Method,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        self._message_queue.put(
            AmqpMessage(
                channel=channel,
                method=method,
                properties=properties,
                body=body,
            )
        )

    def __enter__(self: "AmqpConnection") -> "AmqpConnection":
        self._connection = self._make_connection()

        # declare the queue as durable and set prefetch count to 1
        self._channel = self._connection.channel()
        self._declare_queue(
            queue_name=self.queue,
            durable=True,
            thread_local=True,
        )
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            on_message_callback=self._enqueue_message,
            queue=self.queue,
            auto_ack=False,
        )

        # declare a default resubmit exchange for failed messages
        self._channel.exchange_declare(
            exchange=self.resubmit_exchange,
            exchange_type="direct",
            durable=True,
        )
        self._channel.queue_bind(
            queue=self.queue,
            exchange=self.resubmit_exchange,
        )

        self._start_consuming()
        return self

    def __exit__(self: "AmqpConnection", exc_type, exc_value, traceback) -> None:
        self._stop_consuming()

        if self._connection is not None:
            self._connection.close()

    def _get_delivery_tag(self: "AmqpConnection", message: QueueMessage) -> int:
        return message.attributes["platform"]["method"]["delivery_tag"]

    def forward_message(self: "AmqpConnection", forward: Forward) -> None:
        if self._connection is None or self._channel is None:
            raise RuntimeError("AMQP channel is not established")

        if isinstance(forward.forward_target, str):
            self._declare_queue(
                queue_name=forward.forward_target,
                durable=True,
            )
            self._connection.add_callback_threadsafe(
                functools.partial(
                    self._channel.basic_publish,
                    exchange="",
                    routing_key=forward.forward_target,
                    body=forward.message.data,
                    properties=BasicProperties(
                        headers=forward.message.attributes,
                        delivery_mode=2,  # make message persistent
                    ),
                )
            )
            return

        raise NotImplementedError(
            "Forwarding to non-str targets is not implemented"
        )  # pragma: no cover

    def get_message(self: "AmqpConnection") -> QueueMessage:
        message: AmqpMessage | AmqpSentinel = self._message_queue.get()

        if isinstance(message, AmqpSentinel):
            self._message_queue.shutdown()
            raise ShutdownRequested(*message.args)

        return QueueMessage(
            data=message.body,
            attributes=(
                {
                    key: value
                    for key, value in (message.properties.headers or {}).items()
                    if not key.startswith("x-mqkit-")
                }
                | {
                    "platform": {
                        "channel": {
                            "number": message.channel.channel_number,
                        },
                        "method": {
                            "consumer_tag": message.method.consumer_tag,  # type: ignore
                            "delivery_tag": message.method.delivery_tag,  # type: ignore
                            "exchange": message.method.exchange,  # type: ignore
                            "redelivered": message.method.redelivered,  # type: ignore
                            "routing_key": message.method.routing_key,  # type: ignore
                        },
                        "properties": {
                            "delivery_mode": message.properties.delivery_mode,
                        },
                    }
                }
            ),
        )

    def _make_connection(self: "AmqpConnection") -> PikaBlockingConnection:
        ssl_options: Optional[SSLOptions] = None
        if self.use_ssl:
            raise NotImplementedError(
                "SSL connections are not yet implemented for RabbitMQ"
            )

        return PikaBlockingConnection(
            parameters=ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=self.credentials,
                ssl_options=ssl_options,
            )
        )

    @property
    def resubmit_exchange(self: "AmqpConnection") -> str:
        """
        Property that returns the name of the resubmit exchange for the queue
        associated with this connection.

        Returns:
            str: The name of the resubmit exchange.
        """

        return f"mqkit.resubmit.{slugify(self.queue, separator='_')}"

    def _start_consuming(self: "AmqpConnection") -> None:
        if self._channel is None:
            raise RuntimeError("AMQP channel is not established")

        self._consume_thread = AmqpConsumeThread(
            channel=self._channel,
            daemon=True,
        )
        self._consume_thread.start()  # type: ignore

    def _stop_consuming(self: "AmqpConnection") -> None:
        if self._consume_thread is not None:
            self._consume_thread.stop()
            self._consume_thread.join()

            self._consume_thread = None

    def unblock(self: "AmqpConnection", message: Optional[str] = None) -> None:
        if self._connection is None:
            raise RuntimeError("AMQP connection is not established")

        self._message_queue.put(AmqpSentinel(message))
