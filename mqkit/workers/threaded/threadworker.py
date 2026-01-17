import logging
from logging import Logger
from threading import Thread
from typing import Optional

from ...connections import Connection
from ...endpoints import Endpoint
from ...engines import Engine
from ...errors import NoRetry, ShutdownRequested
from ..worker import Worker
from ...marshal import Forward, QueueMessage


class ThreadWorker(Worker, Thread):
    connection: Connection

    _endpoint: Endpoint
    _logger: Logger
    _stopped: bool = False

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

    def _handle_message(self: "ThreadWorker", message: QueueMessage) -> None:
        try:
            # process the message using the endpoint's handler. the handler may
            # return a Forward object to indicate that a result should be
            # forwarded to another queue
            forward_result: Optional[Forward] = self._endpoint.handle_message(message)
            self.connection.acknowledge_success(message)

            # forward the result as needed
            if forward_result is not None:
                self._logger.debug(
                    f"Forwarding result to {forward_result.forward_target}"
                )
                self.connection.forward_message(
                    forward=forward_result,
                )
        except NoRetry as nr:
            self._logger.warning(
                "Message processing failed with NoRetry"
                + ("" if nr.args == () else f": {nr.args[0]}")
            )
            self.connection.acknowledge_failure(message)
        except Exception as exc:
            self._logger.exception(f"Error while processing message: {exc}")
            self.connection.acknowledge_failure(message)

    def _init_logger(self: "ThreadWorker") -> None:
        self._logger = logging.getLogger(
            name=f"{self.__class__.__module__.split('.')[0]}.{self.__class__.__name__}."
            f"{self._endpoint.__class__.__name__}.{self._endpoint.qualname}"
        )

    def _process_messages(self: "ThreadWorker") -> None:
        while not self._stopped:
            self._handle_message(self._receive_message())

    def _receive_message(self: "ThreadWorker") -> QueueMessage:
        # try getting a message from the queue. this will either block
        # until it succeeds, or raise ShutdownRequested if stop() is called
        message: QueueMessage = self.connection.get_message()
        self._logger.debug(f"Received message of size {len(message.data):,} bytes")
        return message

    def run(self: "ThreadWorker") -> None:
        try:
            with self._engine.connect(
                queue=self._endpoint._queue_name
            ) as self.connection:
                self._process_messages()

        except ShutdownRequested as sr:
            self._logger.warning(
                "Shutdown requested during message processing"
                + ("" if sr.args == () else f": {sr.args[0]}")
            )
            self._stopped = True

    def stop(self: "ThreadWorker", message: Optional[str] = None) -> None:
        self._stopped = True

        # the while loop in run() may be blocked waiting for a message,
        # so we send a dummy message to unblock it
        #
        # if a message is already processing, the while loop will simply
        # exit after the message is processed
        self.connection.unblock(message=message)
