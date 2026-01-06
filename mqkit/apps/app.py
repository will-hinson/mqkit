import asyncio
from typing import Callable, List, Optional

from .concurrencymode import ConcurrencyMode
from ..endpoints import Endpoint, QueueEndpoint
from ..engines import Engine
from ..marshal.codecs import CodecType
from ..workers import Coordinator
from ..workers.threaded import ThreadCoordinator


class App:
    _concurrency_mode: ConcurrencyMode
    _coordinator: Optional[Coordinator] = None
    _endpoints: List[Endpoint] = []

    def __init__(
        self: "App",
        concurrency_mode: ConcurrencyMode | str = ConcurrencyMode.THREAD,
    ) -> None:
        self._concurrency_mode = ConcurrencyMode(concurrency_mode)
        self._endpoints = []

    def _assert_function_compatible(
        self: "App",
        func: Callable,
    ) -> None:
        if not self._is_function_compatible(func):
            raise TypeError(
                ("Async function" if asyncio.iscoroutinefunction(func) else "Function")
                + (
                    f" {func.__name__}() is not compatible with concurrency mode "
                    f"'{self.concurrency_mode.value}'"
                )
            )

    @property
    def concurrency_mode(self: "App") -> ConcurrencyMode:
        return self._concurrency_mode

    def _is_function_compatible(self: "App", func: Callable) -> bool:
        if self.concurrency_mode == ConcurrencyMode.ASYNC:
            return asyncio.iscoroutinefunction(func)
        else:
            return not asyncio.iscoroutinefunction(func)

    def queue(
        self: "App",
        name: str,
        codec: CodecType | str = CodecType.JSON,
    ) -> Callable[[Callable], QueueEndpoint]:
        codec = CodecType(codec)

        def _queue_decorator(func: Callable) -> QueueEndpoint:
            # check that the function is compatible with the selected concurrency mode
            self._assert_function_compatible(func)

            self._endpoints.append(
                QueueEndpoint(
                    queue_name=name,
                    target=func,
                    codec_type=codec,
                )
            )
            return self._endpoints[-1]  # type: ignore

        return _queue_decorator

    def run(self: "App", engine: Engine) -> None:
        # ensure the app is not already running
        if self._coordinator is not None:
            raise RuntimeError("App is already running")

        # instantiate the appropriate coordinator based on the concurrency mode
        if self.concurrency_mode == ConcurrencyMode.THREAD:
            self._run_init_threaded(engine)
        else:
            raise NotImplementedError(
                f"Unimplemented concurrency mode '{self.concurrency_mode.value}'"
            )

        # run the coordinator
        assert self._coordinator is not None
        self._coordinator.run()

    def _run_init_threaded(self: "App", engine: Engine) -> None:
        assert self.concurrency_mode == ConcurrencyMode.THREAD

        if self._coordinator is not None:
            raise RuntimeError("App is already running")

        self._coordinator = ThreadCoordinator(
            endpoints=self._endpoints,
            engine=engine,
        )
