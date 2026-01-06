import logging
from logging import Logger
from threading import Thread
from typing import Any, Optional

from ...endpoints import Endpoint
from ...engines import Engine
from ...errors import NoRetry
from ..worker import Worker
from ...marshal import Forward, QueueMessage


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

                try:
                    forward_result: Optional[Forward] = self._endpoint.handle_message(
                        message
                    )
                    connection.acknowledge_success(message)

                    if forward_result is not None:
                        self._logger.debug(
                            f"Forwarding result to {forward_result.forward_target}"
                        )
                        connection.forward_message(
                            forward=forward_result,
                        )
                except NoRetry as nr:
                    self._logger.warning(
                        "Message processing failed with NoRetry"
                        + ("" if nr.args == () else f": {nr.args[0]}")
                    )
                    connection.acknowledge_failure(message)
                except Exception as exc:
                    self._logger.exception(f"Error while processing message: {exc}")
                    connection.acknowledge_failure(message)
