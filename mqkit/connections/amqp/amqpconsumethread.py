from threading import Event, Lock, Thread
from typing import Optional

from pika.adapters.blocking_connection import BlockingChannel


class AmqpConsumeThread(Thread):
    _channel: BlockingChannel
    _exception: Optional[Exception] = None
    _stop_event: Event

    def __init__(self: "AmqpConsumeThread", channel: BlockingChannel, **kwargs) -> None:
        super().__init__(**kwargs)

        self._channel = channel
        self._stop_event = Event()

    @property
    def error(self: "AmqpConsumeThread") -> Optional[Exception]:
        return self._exception

    def run(self: "AmqpConsumeThread") -> None:
        while not self._stop_event.is_set():
            try:
                self._channel.start_consuming()
            except Exception as ex:
                self._exception = ex
                self.stop()

        self._channel.stop_consuming()

    def stop(self: "AmqpConsumeThread") -> None:
        self._stop_event.set()
