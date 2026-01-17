import asyncio
from typing import Dict, List

from ..coordinator import Coordinator
from ...endpoints import Endpoint
from ...engines import Engine
from .threadworker import ThreadWorker


class ThreadCoordinator(Coordinator):
    def __init__(
        self: "ThreadCoordinator", endpoints: List[Endpoint], engine: Engine
    ) -> None:
        super().__init__(endpoints, engine)

        # assert that all endpoints are compatible with threaded concurrency
        self._assert_endpoints_compatible()

    def _assert_endpoints_compatible(self: "ThreadCoordinator") -> None:
        assert all(
            not asyncio.iscoroutinefunction(endpoint.target)
            for endpoint in self._endpoints
        ), "All endpoints must be compatible with threaded concurrency"

    def run(self: "ThreadCoordinator") -> None:
        workers: Dict[ThreadWorker, Endpoint] = {
            ThreadWorker(endpoint=endpoint, engine=self._engine): endpoint
            for endpoint in self._endpoints
        }
        try:
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
        except KeyboardInterrupt as ki:
            for worker in workers:
                worker.stop(message=f"User interaction ({ki!r})")
            for worker in workers:
                worker.join()
