from threading import Event, Thread

from pika.adapters.blocking_connection import BlockingChannel


class AmqpConsumeThread(Thread):
    _channel: BlockingChannel
    _stop_event: Event

    def __init__(self, channel: BlockingChannel, **kwargs):
        super().__init__(**kwargs)

        self._channel = channel
        self._stop_event = Event()

    def run(self):
        try:
            self._channel.start_consuming()
        except Exception as ex:
            self._exception = ex
        finally:
            if self._channel.is_open:
                self._channel.close()

    def stop(self):
        self._stop_event.set()
        self._channel.connection.add_callback_threadsafe(
            self._channel.stop_consuming,
        )
