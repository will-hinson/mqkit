"""
module mqkit.workers.threaded.threadcoordinator

Defines the ThreadCoordinator class for managing threaded workers
to process messages from multiple endpoints concurrently.
"""

from logging import Logger
import logging
from queue import Queue as ProcessQueue, ShutDown
from typing import Dict, List, Union

from ..coordinator import Coordinator
from ...endpoints import Endpoint
from ...engines import Engine
from ...errors import FunctionTypeError, ShutdownRequested, WorkerTerminatedError
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

    _allow_restart: bool
    _error_queue: ProcessQueue = ProcessQueue()
    _logger: Logger
    _restart_counter: MonotonicCounter
    _serial_number_counter: MonotonicCounter = MonotonicCounter()
    _serial_number: int
    _workers: Dict[ThreadWorker, Endpoint]

    def __init__(
        self: "ThreadCoordinator",
        endpoints: List[Endpoint],
        engine: Engine,
        allow_restart: bool = False,
    ) -> None:
        super().__init__(endpoints, engine)

        # assert that all endpoints are compatible with threaded concurrency
        self._assert_endpoints_compatible()
        self._serial_number = self._serial_number_counter.next()
        self._logger = logging.getLogger(
            f"{root_logger_name}.{'.'.join(self.__class__.__module__.split('.')[1:-1])}."
            f"{self.__class__.__name__}-{self._serial_number}"
        )

        self._allow_restart = allow_restart
        self._error_queue = ProcessQueue()
        self._restart_counter = MonotonicCounter()

    def _assert_endpoints_compatible(self: "ThreadCoordinator") -> None:
        if any(endpoint.is_async for endpoint in self._endpoints):
            raise FunctionTypeError(
                "All endpoints must be compatible with threaded concurrency (async not allowed)"
            )

    def _handle_errors(self: "ThreadCoordinator") -> None:
        while not self._error_queue.is_shutdown:
            error_worker: ThreadWorker
            try:
                error_worker = self._error_queue.get()
            except ShutDown:
                continue

            self._logger.error(
                "Worker %s for endpoint '%s' exited with exception: %s",
                error_worker.name,
                self._workers[error_worker].qualname,
                error_worker.error,
            )

            if not self._allow_restart:
                # if we're not allowed to restart workers, raise an error
                raise WorkerTerminatedError(
                    f"{error_worker.name} terminated and cannot be restarted by coordinator "
                    "(restarts not allowed)"
                ) from error_worker.error

            self._restart_worker(error_worker)

    def _interrupt(
        self: "ThreadCoordinator",
        exception: Union[Exception, KeyboardInterrupt],
    ) -> None:
        self._error_queue.shutdown()
        self._stop_workers(
            exception=exception,
            reason=(
                "User interaction"
                if isinstance(exception, KeyboardInterrupt)
                else str(exception)
            ),
        )

    def _restart_worker(
        self: "ThreadCoordinator",
        worker: ThreadWorker,
    ) -> None:
        endpoint: Endpoint = self._workers[worker]
        self._logger.info(
            "Restarting worker for endpoint '%s' (restart #%d)",
            endpoint.qualname,
            self._restart_counter.next(),
        )

        new_worker: ThreadWorker = ThreadWorker(
            endpoint=endpoint,
            engine=self._engine,
            error_queue=self._error_queue,
        )
        self._workers[new_worker] = endpoint
        del self._workers[worker]

        new_worker.start()
        self._logger.info(
            "Worker for endpoint '%s' restarted successfully",
            endpoint.qualname,
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
        self._handle_errors()

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
            ThreadWorker(
                endpoint=endpoint,
                engine=self._engine,
                error_queue=self._error_queue,
            ): endpoint
            for endpoint in self._endpoints
        }

        try:
            self._start_workers()
        except KeyboardInterrupt as ki:  # pragma: no cover
            self._interrupt(ki)
        except WorkerTerminatedError as wti:
            self._interrupt(wti)
            raise

    def stop(
        self: "ThreadCoordinator",
    ) -> None:
        self._interrupt(
            ShutdownRequested(
                "Shutdown requested by application",
            )
        )
