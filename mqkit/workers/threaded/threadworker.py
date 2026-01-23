"""
module mqkit.workers.threaded.threadworker

A module defining a threaded worker for processing messages from a message queue.
"""

import logging
from logging import Logger
from threading import Event, Thread
from typing import Optional

from ...connections import Connection
from ...endpoints import Endpoint
from ...engines import Engine
from ...errors import NoRetry, ShutdownRequested
from ...messaging import Forward, QueueMessage
from .monotoniccounter import MonotonicCounter
from ..worker import Worker


class ThreadWorker(Worker, Thread):
    """
    class ThreadWorker

    A worker that processes messages from a message queue using threading.
    """

    connection: Connection
    error: Optional[Exception] = None

    _counter: MonotonicCounter = MonotonicCounter()

    _endpoint: Endpoint
    _logger: Logger
    _started_event: Event = Event()
    _stopped: bool = False

    def __init__(
        self: "ThreadWorker",
        endpoint: Endpoint,
        engine: Engine,
    ) -> None:
        Thread.__init__(self, name=f"ThreadWorker-{self._counter.next()}")
        Worker.__init__(self)

        self._endpoint = endpoint
        self._engine = engine
        self._init_logger()

    def _handle_message(self: "ThreadWorker", message: QueueMessage) -> None:
        # pylint: disable=broad-except
        try:
            # process the message using the endpoint's handler. the handler may
            # return a Forward object to indicate that a result should be
            # forwarded to another queue
            forward_result: Optional[Forward] = self._endpoint.handle_message(message)
            self.connection.acknowledge_success(message)

            # forward the result as needed
            if forward_result is not None:
                self._logger.debug(
                    "Forwarding result to %s", forward_result.forward_target
                )
                self.connection.forward_message(
                    forward=forward_result,
                )
        except NoRetry as nr:
            self._logger.warning(
                "Message processing failed with NoRetry%s",
                "" if len(nr.args) == 0 else f": {nr.args[0]}",
            )
            self.connection.acknowledge_failure(message)
        except Exception as exc:  # pragma: no cover
            # NOTE: we will want to add coverage here in the future once we have
            # retry logic implemented
            self._logger.exception("Error while processing message: %s", exc)
            self.connection.acknowledge_failure(message)

    def _init_logger(self: "ThreadWorker") -> None:
        self._logger = logging.getLogger(
            name=f"{self.__class__.__module__.split('.', maxsplit=1)[0]}.{self.__class__.__name__}."
            f"{self._endpoint.__class__.__name__}.{self._endpoint.qualname}"
        )

    def _process_messages(self: "ThreadWorker") -> None:
        while not self._stopped:
            self._handle_message(self._receive_message())

    def _receive_message(self: "ThreadWorker") -> QueueMessage:
        # try getting a message from the queue. this will either block
        # until it succeeds, or raise ShutdownRequested if stop() is called
        message: QueueMessage = self.connection.get_message()
        self._logger.debug(
            "Received message of size %s byte%s",
            f"{len(message.data):,}",
            "" if len(message.data) == 1 else "s",
        )
        return message

    def run(self: "ThreadWorker") -> None:
        # pylint: disable=broad-exception-caught
        try:
            with self._engine.connect(
                queue=self._endpoint.queue_name,
                persistent=self._endpoint.is_persistent,
                auto_delete=self._endpoint.is_auto_delete,
            ) as self.connection:
                self._started_event.set()
                self._process_messages()

        except ShutdownRequested as sr:
            self._logger.warning(
                "Shutdown requested for %s during message processing%s",
                self.name,
                "" if len(sr.args) == 0 else f": {sr.args[0]}",
            )
            self._stopped = True

        except Exception as exc:  # pragma: no cover
            self._logger.exception("Unhandled exception in %s: %s", self.name, exc)
            self.error = exc
            self._stopped = True

        self._started_event.set()

    def stop(self: "ThreadWorker", message: Optional[str] = None) -> None:
        """
        Stops the worker by unblocking the connection.

        Args:
            message (Optional[str]): An optional message to send when unblocking.

        Returns:
            None

        Raises:
            Nothing
        """

        self._started_event.wait()
        self._stopped = True

        # the while loop in run() may be blocked waiting for a message,
        # so we send a dummy message to unblock it
        #
        # if a message is already processing, the while loop will simply
        # exit after the message is processed
        self.connection.unblock(message=message)

    @property
    def stopped(self: "ThreadWorker") -> bool:
        """
        Indicates whether the worker has been stopped.

        Args:
            None

        Returns:
            bool: True if the worker has been stopped, False otherwise.

        Raises:
            Nothing
        """

        return self._stopped
