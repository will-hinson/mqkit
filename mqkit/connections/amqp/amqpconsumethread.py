from threading import Thread
from typing import Optional

from pika.adapters.blocking_connection import BlockingChannel


class AmqpConsumeThread(Thread):
    _channel: BlockingChannel
    _exception: Optional[Exception] = None

    def __init__(self, channel: BlockingChannel, **kwargs):
        super().__init__(**kwargs)

        self._channel = channel

    @property
    def error(self: "AmqpConsumeThread") -> Optional[Exception]:
        return self._exception

    def run(self: "AmqpConsumeThread") -> None:
        try:
            self._channel.start_consuming()
        except Exception as ex:
            self._exception = ex
        finally:
            if self._channel.is_open:
                self._channel.close()

    def stop(self: "AmqpConsumeThread") -> None:
        self._channel.connection.add_callback_threadsafe(
            self._channel.stop_consuming,
        )
