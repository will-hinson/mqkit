import functools
from queue import Queue
import ssl
from typing import Optional

from pika import BasicProperties, ConnectionParameters, PlainCredentials, SSLOptions
from pika import BlockingConnection as PikaBlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method
from pydantic import BaseModel, PrivateAttr

from .amqpconsumethread import AmqpConsumeThread
from .amqpmessage import AmqpMessage
from ..connection import Connection
from ...marshal import QueueMessage


class AmqpConnection(Connection, BaseModel):
    host: str
    port: int
    vhost: str
    credentials: PlainCredentials
    queue: str
    use_ssl: bool = False

    _channel: Optional[BlockingChannel] = PrivateAttr(default=None)
    _connection: Optional[PikaBlockingConnection] = PrivateAttr(default=None)
    _consume_thread: Optional[AmqpConsumeThread] = PrivateAttr(default=None)
    _message_queue: Queue = PrivateAttr(default_factory=Queue)

    class Config:
        arbitrary_types_allowed = True

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
        ssl_options: Optional[SSLOptions] = None
        if self.use_ssl:
            ssl_options = SSLOptions(context=ssl.create_default_context())

        self._connection = PikaBlockingConnection(
            parameters=ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=self.credentials,
                ssl_options=ssl_options,
            )
        )

        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.queue, durable=True)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            on_message_callback=self._enqueue_message,
            queue=self.queue,
            auto_ack=False,
        )

        self._consume_thread = AmqpConsumeThread(
            channel=self._channel,
            daemon=True,
        )
        self._consume_thread.start()  # type: ignore

        return self

    def __exit__(self: "AmqpConnection", exc_type, exc_value, traceback) -> None:
        if self._consume_thread is not None:
            self._consume_thread.stop()
            self._consume_thread.join()

        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()

    def _get_delivery_tag(self: "AmqpConnection", message: QueueMessage) -> int:
        return message.attributes["platform"]["method"]["delivery_tag"]

    def get_message(self: "AmqpConnection") -> QueueMessage:
        message: AmqpMessage = self._message_queue.get()

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
