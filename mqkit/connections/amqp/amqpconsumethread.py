"""
module mqkit.connections.amqp.amqpconsumethread

Defines the AmqpConsumeThread class for consuming AMQP messages in a separate thread.
"""

from threading import Thread
from typing import Optional

from pika.adapters.blocking_connection import BlockingChannel


class AmqpConsumeThread(Thread):
    """
    class AmqpConsumeThread

    A thread that consumes AMQP messages from a BlockingChannel.
    """

    _channel: BlockingChannel
    _exception: Optional[Exception] = None

    def __init__(self, channel: BlockingChannel, **kwargs):
        super().__init__(**kwargs)

        self._channel = channel

    @property
    def error(self: "AmqpConsumeThread") -> Optional[Exception]:  # pragma: no cover
        """
        Gets any exception that occurred during message consumption.

        Args:
            None

        Returns:
            Optional[Exception]: The exception that occurred, or None if no exception
                occurred.

        Raises:
            Nothing
        """

        return self._exception

    def run(self: "AmqpConsumeThread") -> None:  # pragma: no cover
        """
        Starts consuming AMQP messages from the target channel.

        Args:
            None

        Returns:
            None

        Raises:
            Nothing
        """

        # pylint: disable=broad-except
        try:
            self._channel.start_consuming()
        except Exception as ex:
            self._exception = ex
        finally:
            if self._channel.is_open:
                self._channel.close()

    def stop(self: "AmqpConsumeThread") -> None:
        """
        Stops this thread from consuming AMQP messages from the target channel.

        Args:
            None

        Returns:
            None

        Raises:
            Nothing
        """

        self._channel.connection.add_callback_threadsafe(
            self._channel.stop_consuming,
        )
