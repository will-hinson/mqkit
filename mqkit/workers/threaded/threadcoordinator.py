"""
module mqkit.workers.threaded.threadcoordinator

Defines the ThreadCoordinator class for managing threaded workers
to process messages from multiple endpoints concurrently.
"""

from logging import Logger
import logging
from typing import Dict, List, Union

from ..coordinator import Coordinator
from ...endpoints import Endpoint
from ...engines import Engine
from ...errors import FunctionTypeError, ShutdownRequested
from ...logging import root_logger_name
from .monotoniccounter import MonotonicCounter
from .threadworker import ThreadWorker


class ThreadCoordinator(Coordinator):
    """
    class ThreadCoordinator

    A coordinator that manages workers for processing messages from
    multiple endpoints concurrently using threading.
    """

    # pylint: disable=too-few-public-methods

    _counter: MonotonicCounter = MonotonicCounter()
    _logger: Logger
    _workers: Dict[ThreadWorker, Endpoint]
    _serial_number: int

    def __init__(
        self: "ThreadCoordinator", endpoints: List[Endpoint], engine: Engine
    ) -> None:
        super().__init__(endpoints, engine)

        # assert that all endpoints are compatible with threaded concurrency
        self._assert_endpoints_compatible()
        self._serial_number = self._counter.next()
        self._logger = logging.getLogger(
            f"{root_logger_name}.{'.'.join(self.__class__.__module__.split('.')[1:-1])}."
            f"{self.__class__.__name__}-{self._serial_number}"
        )

    def _assert_endpoints_compatible(self: "ThreadCoordinator") -> None:
        if any(endpoint.is_async for endpoint in self._endpoints):
            raise FunctionTypeError(
                "All endpoints must be compatible with threaded concurrency (async not allowed)"
            )

    def _interrupt(
        self: "ThreadCoordinator",
        exception: Union[Exception, KeyboardInterrupt],
    ) -> None:
        self._stop_workers(
            exception=exception,
            reason="User interaction",
        )

    def _start_workers(self: "ThreadCoordinator") -> None:
        self._logger.debug(
            "Starting ThreadCoordinator with %d worker%s",
            len(self._workers),
            "s" if len(self._workers) != 1 else "",
        )
        for worker in self._workers:
            worker.start()
        self._logger.debug("All workers started")
        for worker in self._workers:
            worker.join()

    def _stop_workers(
        self: "ThreadCoordinator",
        exception: Union[Exception, KeyboardInterrupt],
        reason: str,
    ) -> None:
        self._logger.warning("KeyboardInterrupt received, stopping workers")

        for worker in self._workers:
            worker.stop(message=f"{reason} ({exception!r})")
        for worker in self._workers:
            worker.join()

        self._logger.warning("All workers stopped")

    def run(self: "ThreadCoordinator") -> None:
        """
        Run the ThreadCoordinator by starting and managing ThreadWorkers for each
        endpoint. Handles graceful shutdown on user interruption via KeyboardInterrupt.

        Args:
            None

        Returns:
            None

        Raises:
            Nothing
        """

        self._workers = {
            ThreadWorker(endpoint=endpoint, engine=self._engine): endpoint
            for endpoint in self._endpoints
        }

        try:
            self._start_workers()
        except KeyboardInterrupt as ki:  # pragma: no cover
            self._interrupt(ki)

    def stop(
        self: "ThreadCoordinator",
    ) -> None:
        self._interrupt(
            ShutdownRequested(
                "Shutdown requested by application",
            )
        )
