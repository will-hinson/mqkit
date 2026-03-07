"""
module mqkit.connections.amqp.amqpconnection

Defines the AmqpConnection class for managing AMQP connections in message
queue applications. This connection class is intended to be returned by
the RabbitMqEngine class
"""

import functools
from queue import Queue as ProcessQueue
import threading
from typing import ClassVar, Dict, List, Optional, Set

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
from ...declarations import Declaration, ExchangeDeclaration, QueueDeclaration
from ...errors import ShutdownRequested
from ...messaging import (
    Attributes,
    Exchange,
    ExchangeType,
    Forward,
    Queue,
    QueueMessage,
)


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
    queue: Queue
    use_ssl: bool = False

    _channel: Optional[BlockingChannel] = PrivateAttr(default=None)
    _connection: Optional[PikaBlockingConnection] = PrivateAttr(default=None)
    _consume_thread: Optional[AmqpConsumeThread] = PrivateAttr(default=None)
    _declared_exchanges: Set[str] = PrivateAttr(default_factory=set)
    _declared_queues: Set[str] = PrivateAttr(default_factory=set)
    _message_queue: ProcessQueue = PrivateAttr(default_factory=ProcessQueue)

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

    def _declare_exchange(
        self: "AmqpConnection",
        exchange: Exchange,
        thread_local: bool = False,
    ) -> None:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")

        # if we've already declared this exchange, skip it
        if exchange.name in self._declared_exchanges:  # pragma: no cover
            return

        done_event: threading.Event = threading.Event()
        declare_exception: Optional[Exception] = None

        def declare() -> None:
            if self._channel is None:  # pragma: no cover
                raise RuntimeError("AMQP channel is not established")

            # pylint: disable=broad-exception-caught
            try:
                self._channel.exchange_declare(
                    exchange=exchange.name,
                    exchange_type=exchange.type.value,
                    durable=exchange.persistent,
                    auto_delete=exchange.auto_delete,
                )
            except Exception as exc:  # pragma: no cover
                nonlocal declare_exception
                declare_exception = exc
            finally:
                done_event.set()

        if not thread_local:  # pragma: no cover
            self._connection.add_callback_threadsafe(declare)
            done_event.wait()
        else:
            declare()

        if declare_exception is not None:  # pragma: no cover
            raise declare_exception

        self._declared_exchanges.add(exchange.name)

    def _declare_queue(
        self: "AmqpConnection",
        queue: Queue,
        thread_local: bool = False,
    ) -> None:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")

        # if we've already declared this queue, skip it
        if queue.name in self._declared_queues:  # pragma: no cover
            return

        done_event: threading.Event = threading.Event()

        def declare() -> None:
            if self._channel is None:  # pragma: no cover
                raise RuntimeError("AMQP channel is not established")

            self._channel.queue_declare(  # pyright: ignore[reportOptionalMemberAccess]
                queue=queue.name,
                durable=queue.persistent,
                auto_delete=queue.auto_delete,
            )
            done_event.set()

        if not thread_local:  # pragma: no cover
            self._connection.add_callback_threadsafe(declare)
            done_event.wait()
        else:
            declare()

        self._declared_queues.add(queue.name)

    def _declare_exchange_with_bindings(
        self: "AmqpConnection",
        exchange_declaration: ExchangeDeclaration,
    ) -> None:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")

        self._declare_exchange(
            exchange_declaration.exchange,
            thread_local=True,
        )

        for binding in exchange_declaration.bindings:
            if isinstance(binding.bound_resource, Queue):
                self._channel.queue_bind(
                    queue=binding.bound_resource.name,
                    exchange=exchange_declaration.exchange.name,
                    routing_key=binding.topic,
                )
                continue
            if isinstance(binding.bound_resource, Exchange):
                self._channel.exchange_bind(
                    destination=binding.bound_resource.name,
                    source=exchange_declaration.exchange.name,
                    routing_key=binding.topic,
                )
                continue

            raise NotImplementedError(  # pragma: no cover
                f"Binding resources of type {type(binding.bound_resource).__name__} "
                "is not implemented"
            )

    def _declare_resubmit_exchange(
        self: "AmqpConnection",
        target_queue: Queue,
        thread_local: bool = False,
    ) -> Exchange:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")

        resubmit_exchange: Exchange = Exchange(
            name=self._get_resubmit_exchange(target_queue.name),
            type=ExchangeType.FANOUT,
            persistent=target_queue.persistent,
        )

        # declare the exchange and bind the queue to it
        self._declare_exchange(
            resubmit_exchange,
            thread_local=thread_local,
        )
        if thread_local:
            self._channel.queue_bind(
                queue=target_queue.name,
                exchange=resubmit_exchange.name,
            )
        else:
            self._connection.add_callback_threadsafe(
                functools.partial(
                    self._channel.queue_bind,
                    queue=target_queue.name,
                    exchange=resubmit_exchange.name,
                )
            )

        # return the exchange object we instantiated
        return resubmit_exchange

    def declare_resources(self: "AmqpConnection", resources: List[Declaration]) -> None:
        if self._connection is None or self._channel is None:
            self._connection = self._make_connection()
            self._channel = self._connection.channel()

        try:
            for resource in resources:
                if isinstance(resource, ExchangeDeclaration):
                    self._declare_exchange_with_bindings(resource)
                    continue
                if isinstance(resource, QueueDeclaration):
                    self._declare_queue(
                        resource.queue,
                        thread_local=True,
                    )
                    continue

                raise NotImplementedError(  # pragma: no cover
                    f"Declaring resources of type {type(resource).__name__} is not implemented"
                )
        finally:
            if self._connection is not None and not self._connection.is_closed:
                self._connection.close()
                self._connection = None
                self._channel = None

    def _decode_retry_count(
        self: "AmqpConnection",
        properties: BasicProperties,
        key: str = "x-mqkit-retry-count",
    ) -> int:
        if properties.headers and key in properties.headers:
            try:
                return int(properties.headers[key])
            except (ValueError, TypeError):
                pass

        return 0

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

        # declare the queue as durable if needed and set prefetch count to 1
        self._channel = self._connection.channel()
        self._declare_queue(
            self.queue,
            thread_local=True,
        )
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            on_message_callback=self._enqueue_message,
            queue=self.queue.name,
            auto_ack=False,
        )

        # declare a default resubmit exchange for failed messages
        self._declare_resubmit_exchange(
            self.queue,
            thread_local=True,
        )

        self._start_consuming()
        return self

    def __exit__(self: "AmqpConnection", exc_type, exc_value, traceback) -> None:
        self._stop_consuming()

        if self._connection is not None and not self._connection.is_closed:
            self._connection.close()

    def _get_delivery_tag(self: "AmqpConnection", message: QueueMessage) -> int:
        return message.attributes.platform["method"]["delivery_tag"]  # type: ignore

    def forward_message(self: "AmqpConnection", forward: Forward) -> None:
        if self._connection is None or self._channel is None:
            raise RuntimeError("AMQP channel is not established")

        if isinstance(forward.forward_target, Queue):
            self._forward_message_to_queue(forward)
            return

        if isinstance(forward.forward_target, Exchange):
            self._declare_exchange(forward.forward_target)
            self._connection.add_callback_threadsafe(
                functools.partial(
                    self._channel.basic_publish,
                    exchange=forward.forward_target.name,
                    routing_key=forward.message.attributes.topic or "",
                    body=forward.message.data,
                    properties=BasicProperties(
                        headers=self._get_forward_message_headers(forward),
                        delivery_mode=2,  # make message persistent
                    ),
                )
            )
            return

        raise NotImplementedError(
            f"Forwarding to targets of type {type(forward.forward_target).__name__} "
            "is not implemented"
        )  # pragma: no cover

    def _forward_message_to_queue(
        self: "AmqpConnection",
        forward: Forward,
    ) -> None:
        if self._connection is None or self._channel is None:  # pragma: no cover
            raise RuntimeError("AMQP channel is not established")
        if not isinstance(forward.forward_target, Queue):  # pragma: no cover
            raise TypeError("Forward target must be a Queue")

        # if a topic was specified, we cannot forward to a queue directly. ensure that
        # a resumbit exchange exists for the target queue and publish to that instead
        if forward.message.attributes.topic is not None:
            self.forward_message(
                Forward(
                    forward_target=self._declare_resubmit_exchange(
                        forward.forward_target
                    ),
                    message=forward.message,
                )
            )
            return

        # if no topic was specified, just publish to the queue directly using the
        # default exchange

        # NOTE: support forward target durability options later. we can maybe infer
        # from other queue definitions
        self._declare_queue(forward.forward_target)
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._channel.basic_publish,
                exchange="",
                routing_key=forward.forward_target.name,
                body=forward.message.data,
                properties=BasicProperties(
                    headers=self._get_forward_message_headers(forward),
                    delivery_mode=2,  # make message persistent
                ),
            )
        )

    def _get_forward_message_headers(
        self: "AmqpConnection",
        forward: Forward,
    ) -> Dict[str, str]:
        headers: Dict[str, str] = forward.message.attributes.headers.copy()

        if forward.message.attributes.retry_count > 0:
            headers["x-mqkit-retry-count"] = str(forward.message.attributes.retry_count)

        return headers

    def get_message(self: "AmqpConnection") -> QueueMessage:
        message: AmqpMessage | AmqpSentinel = self._message_queue.get()

        if isinstance(message, AmqpSentinel):
            self._message_queue.shutdown()
            raise ShutdownRequested(*message.args)

        return QueueMessage(
            data=message.body,
            attributes=Attributes(
                headers={
                    key: str(value)
                    for key, value in (message.properties.headers or {}).items()
                    if key.startswith("x-mqkit-") and key != "x-mqkit-exception-history"
                },
                platform={
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
                },
                forwarded=(
                    bool(message.properties.headers.get("x-mqkit-forwarded", "false"))
                    if message.properties.headers
                    else False
                ),
                origin_queue=(
                    str(message.properties.headers.get("x-mqkit-origin-queue"))
                    if message.properties.headers
                    else None
                ),
                topic=(
                    message.method.routing_key  # type: ignore
                    if message.method.exchange != ""  # type: ignore
                    else None
                ),
                retry_count=self._decode_retry_count(message.properties),
                previous_retry_count=self._decode_retry_count(
                    message.properties,
                    key="x-mqkit-previous-retry-count",
                ),
                is_dead_letter=(
                    (
                        str(
                            message.properties.headers.get(
                                "x-mqkit-dead-letter", "false"
                            )
                        ).lower()
                        == "true"
                    )
                    if message.properties.headers
                    else False
                ),
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

    def _get_resubmit_exchange(self: "AmqpConnection", queue_name: str) -> str:
        return f"mqkit.resubmit.{slugify(queue_name, separator='_')}"

    @property
    def resubmit_exchange(self: "AmqpConnection") -> str:  # pragma: no cover
        """
        Property that returns the name of the resubmit exchange for the queue
        associated with this connection.

        Returns:
            str: The name of the resubmit exchange.
        """

        return self._get_resubmit_exchange(self.queue.name)

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

    def submit_message(self: "AmqpConnection", message: "QueueMessage") -> None:
        self.forward_message(
            Forward(
                forward_target=self.queue,
                message=message,
            )
        )

    def unblock(self: "AmqpConnection", message: Optional[str] = None) -> None:
        if self._connection is None:
            raise RuntimeError("AMQP connection is not established")

        self._message_queue.put(AmqpSentinel(message))
