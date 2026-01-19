"""
module mqkit.apps.app

Contains the definition of the App class for building message queue applications.
"""

import asyncio
from typing import Callable, Dict, List, Optional

from .concurrencymode import ConcurrencyMode
from ..endpoints import Endpoint, QueueEndpoint
from ..engines import Engine
from ..events import AppEventType
from ..marshal.codecs import CodecType
from ..workers import Coordinator
from ..workers.threaded import ThreadCoordinator


class App:
    """
    class App

    Represents a message queue application. Implements a FastAPI-style interface
    for user code to define endpoints and event handlers.
    """

    _concurrency_mode: ConcurrencyMode
    _coordinator: Optional[Coordinator] = None
    _endpoints: List[Endpoint] = []
    _event_functions: Dict[AppEventType, Callable] = {}

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
        """
        Property that returns the concurrency mode of the application.

        Returns:
            ConcurrencyMode: The concurrency mode of the application.
        """

        return self._concurrency_mode

    def _handle_event(self: "App", event_type: AppEventType) -> None:
        func: Optional[Callable] = self._event_functions.get(event_type)
        if func is not None:
            func()

    def _is_function_compatible(self: "App", func: Callable) -> bool:
        if self.concurrency_mode == ConcurrencyMode.ASYNC:
            return asyncio.iscoroutinefunction(func)

        return not asyncio.iscoroutinefunction(func)

    def on_start(
        self: "App",
        func: Callable[[], None],
    ) -> Callable[[], None]:
        """
        Registers a callback function to be called when the application starts.

        Args:
            func (Callable[[], None]): The callback function to register.

        Returns:
            Callable[[], None]: The registered callback function.

        Raises:
            TypeError: If the function is not compatible with the selected concurrency mode.
        """

        self._assert_function_compatible(func)

        self._event_functions[AppEventType.START] = func
        return func

    def queue(
        self: "App",
        name: str,
        codec: CodecType | str = CodecType.JSON,
        forward_to: Optional[str] = None,
    ) -> Callable[[Callable], QueueEndpoint]:
        """
        Decorator to register a function as a queue endpoint.

        Args:
            name (str): The name of the queue.
            codec (CodecType | str): The codec type for message serialization.
                Defaults to CodecType.JSON.
            forward_to (Optional[str]): The name of the queue to forward results to.
                Defaults to None.

        Returns:
            Callable[[Callable], QueueEndpoint]: The decorator function.

        Raises:
            TypeError: If the function is not compatible with the selected concurrency mode.
        """

        codec = CodecType(codec)

        def _queue_decorator(func: Callable) -> QueueEndpoint:
            # check that the function is compatible with the selected concurrency mode
            self._assert_function_compatible(func)

            self._endpoints.append(
                QueueEndpoint(
                    queue_name=name,
                    target=func,
                    codec_type=codec,
                    forward_to=forward_to,
                )
            )
            return self._endpoints[-1]  # type: ignore

        return _queue_decorator

    def run(self: "App", engine: Engine) -> None:
        """
        Runs the application using the specified message queue connection engine.

        Args:
            engine (Engine): The message queue connection engine to use.

        Returns:
            None

        Raises:
            RuntimeError: If the application is already running.
            NotImplementedError: If the selected concurrency mode is not implemented.
        """

        # ensure the app is not already running
        if self._coordinator is not None:
            raise RuntimeError("App is already running")

        # instantiate the appropriate coordinator based on the concurrency mode
        if self.concurrency_mode == ConcurrencyMode.THREAD:
            self._init_threaded(engine)
        else:
            raise NotImplementedError(
                f"Unimplemented concurrency mode '{self.concurrency_mode.value}'"
            )

        # run the coordinator
        assert self._coordinator is not None
        self._handle_event(AppEventType.START)
        self._coordinator.run()

    def _init_threaded(self: "App", engine: Engine) -> None:
        assert self.concurrency_mode == ConcurrencyMode.THREAD

        if self._coordinator is not None:
            raise RuntimeError("App is already running")

        self._coordinator = ThreadCoordinator(
            endpoints=self._endpoints,
            engine=engine,
        )
