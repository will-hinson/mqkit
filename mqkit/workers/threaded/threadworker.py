import logging
from logging import Logger
from threading import Thread

from ...endpoints import Endpoint
from ...engines import Engine
from ..worker import Worker
from ...marshal import QueueMessage


class ThreadWorker(Worker, Thread):
    _endpoint: Endpoint
    _logger: Logger

    def __init__(
        self: "ThreadWorker",
        endpoint: Endpoint,
        engine: Engine,
    ) -> None:
        Thread.__init__(self)
        Worker.__init__(self)

        self._endpoint = endpoint
        self._engine = engine
        self._init_logger()

    def _init_logger(self: "ThreadWorker") -> None:
        self._logger = logging.getLogger(
            name=f"{self.__class__.__module__.split('.')[0]}.{self.__class__.__name__}."
            f"{self._endpoint.__class__.__name__}.{self._endpoint.qualname}"
        )

    def run(self: "ThreadWorker") -> None:
        with self._engine.connect(queue=self._endpoint._queue_name) as connection:
            while True:
                message: QueueMessage = connection.get_message()
                self._logger.debug(
                    f"Received message of size {len(message.data):,} bytes"
                )
                self._endpoint.handle_message(message)

                # TODO: need to acknowledge the message
                connection.acknowledge_success(message)
