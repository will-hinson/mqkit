import asyncio
from logging import Logger
import logging
from typing import Dict, List, Union

from ..coordinator import Coordinator
from ...endpoints import Endpoint
from ...engines import Engine
from .threadworker import ThreadWorker


class ThreadCoordinator(Coordinator):
    _logger: Logger

    def __init__(
        self: "ThreadCoordinator", endpoints: List[Endpoint], engine: Engine
    ) -> None:
        super().__init__(endpoints, engine)

        # assert that all endpoints are compatible with threaded concurrency
        self._assert_endpoints_compatible()
        self._logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

    def _assert_endpoints_compatible(self: "ThreadCoordinator") -> None:
        assert all(
            not asyncio.iscoroutinefunction(endpoint.target)
            for endpoint in self._endpoints
        ), "All endpoints must be compatible with threaded concurrency"

    def _stop_workers(
        self: "ThreadCoordinator",
        workers: Dict[ThreadWorker, Endpoint],
        exception: Union[Exception, KeyboardInterrupt],
        reason: str,
    ) -> None:
        self._logger.warning("KeyboardInterrupt received, stopping workers")

        for worker in workers:
            worker.stop(message=f"{reason} ({exception!r})")
        for worker in workers:
            worker.join()

        self._logger.warning("All workers stopped")

    def run(self: "ThreadCoordinator") -> None:
        workers: Dict[ThreadWorker, Endpoint] = {
            ThreadWorker(endpoint=endpoint, engine=self._engine): endpoint
            for endpoint in self._endpoints
        }
        try:
            self._logger.debug(
                f"Starting ThreadCoordinator with {len(workers)} "
                f"worker{'s' if len(workers) != 1 else ''}"
            )
            for worker in workers:
                worker.start()
            self._logger.debug("All workers started")
            for worker in workers:
                worker.join()
        except KeyboardInterrupt as ki:
            self._stop_workers(
                workers=workers,
                exception=ki,
                reason="User interaction",
            )
