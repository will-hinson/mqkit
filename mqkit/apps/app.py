"""
module mqkit.apps.app

Contains the definition of the App class for building message queue applications.
"""

import inspect
import logging
from logging import Logger
from typing import Callable, Dict, List, Optional, Set, Type, Union
import warnings

from pydantic import ValidationError

from .concurrencymode import ConcurrencyMode
from ..declarations import Declaration, ExchangeDeclaration, QueueDeclaration
from ..endpoints import Endpoint, EndpointFactory, QueueEndpoint
from ..endpoints.config import QueueEndpointConfig
from ..endpoints.endpoint import EndpointDecodeException, EndpointExceptionHandler
from ..engines import Engine
from ..errors import DecodeError, FunctionTypeError
from ..events import AppEventType
from ..logging import root_logger_name
from ..marshal.codecs import CodecType
from ..messaging import Exchange, ExchangeType, ForwardTarget, Queue
from ..messaging.retry import NoRetryStrategy, RetryStrategy
from ..warnings import UnboundQueueWarning
from ..workers import Coordinator
from ..workers.threaded import ThreadCoordinator


class App:
    """
    class App

    Represents a message queue application. Implements a FastAPI-style interface
    for user code to define endpoints and event handlers.
    """

    _codec_type: CodecType
    _concurrency_mode: ConcurrencyMode
    _coordinator: Optional[Coordinator] = None
    _declarations: List[Declaration]
    _endpoints: List[Endpoint] = []
    _event_functions: Dict[AppEventType, Callable] = {}
    _logger: Logger
    _started: bool = False

    def __init__(
        self: "App",
        concurrency_mode: ConcurrencyMode | str = ConcurrencyMode.THREAD,
        codec: CodecType | str = CodecType.JSON,
        logger: Optional[Logger] = None,
    ) -> None:
        self._concurrency_mode = ConcurrencyMode(concurrency_mode)
        self._declarations = []
        self._endpoints = []
        self._codec_type = CodecType(codec)

        self._logger = logger or self._make_default_logger()

    def _assert_function_compatible(
        self: "App",
        func: Callable,
    ) -> None:
        if not self._is_function_compatible(func):
            raise FunctionTypeError(
                ("Async function" if inspect.iscoroutinefunction(func) else "Function")
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

    def create_resources(self: "App", engine: Engine) -> None:
        """
        Returns the list of declared resources for the application.

        Args:
            None

        Returns:
            List[Declaration]: The list of declared resources.
        """

        engine.declare_resources(self._declarations)

    def declare(self: "App", resource: Union[Queue, Exchange]) -> Declaration:
        """
        Declares a message queue or exchange resource for the application.

        Args:
            resource (Union[Queue, Exchange]): The message queue or exchange to declare.

        Returns:
            None
        """

        if self._started:
            raise RuntimeError("Cannot declare resources after the app has started")

        declaration: Declaration
        if isinstance(resource, Exchange):
            declaration = ExchangeDeclaration(
                exchange=Exchange(
                    name=resource.name,
                    type=resource.type,
                    persistent=resource.persistent,
                    auto_delete=resource.auto_delete,
                ),
            )
            self._declarations.append(declaration)
            return declaration
        if isinstance(resource, Queue):
            declaration = QueueDeclaration(
                queue=Queue(
                    name=resource.name,
                    persistent=resource.persistent,
                    auto_delete=resource.auto_delete,
                ),
            )
            self._declarations.append(declaration)
            return declaration

        raise TypeError(f"Cannot declare resource of type {type(resource).__name__}")

    def exchange(
        # pylint: disable=redefined-builtin
        self: "App",
        name: str,
        type: Union[ExchangeType, str],
        persistent: bool = True,
        auto_delete: bool = False,
    ) -> ExchangeDeclaration:
        """
        Declares an exchange resource for the application.

        Args:
            name (str): The name of the exchange.
            type (ExchangeType): The type of the exchange.
            persistent (bool): Whether the exchange is persistent. Defaults to True.
            auto_delete (bool): Whether the exchange is auto-delete. Defaults to False.

        Returns:
            None
        """

        declaration: Declaration = self.declare(
            Exchange(
                name=name,
                type=type,
                persistent=persistent,
                auto_delete=auto_delete,
            )
        )
        if not isinstance(declaration, ExchangeDeclaration):  # pragma: no cover
            raise TypeError(
                "Declared exchange did not return ExchangeDeclaration instance"
            )

        return declaration

    def _handle_event(self: "App", event_type: AppEventType) -> None:
        func: Optional[Callable] = self._event_functions.get(event_type)
        if func is not None:
            func()

    def _is_function_compatible(self: "App", func: Callable) -> bool:
        if self.concurrency_mode == ConcurrencyMode.ASYNC:  # pragma: no cover
            return inspect.iscoroutinefunction(func)

        return not inspect.iscoroutinefunction(func)

    @property
    def logger(self: "App") -> Logger:
        """
        Property that returns the logger for the application.

        Returns:
            Logger: The logger for the application.
        """

        return self._logger

    def _make_default_logger(self: "App") -> Logger:
        return logging.getLogger(root_logger_name)

    def on_event(
        self: "App",
        event_type: AppEventType,
    ) -> Callable[[Callable], Callable]:
        """
        Registers a callback function to be called for a specific application event.

        Args:
            event_type (AppEventType): The type of event to register the callback for.
            func (Callable[[], None]): The callback function to register.

        Returns:
            Callable[[], None]: The registered callback function.

        Raises:
            FunctionTypeError: If the function is not compatible with the selected concurrency mode.
        """

        if self._started:
            raise RuntimeError(
                "Cannot register event handlers after the app has started"
            )

        def _on_event_decorator(
            func: Callable[[], None],
        ) -> Callable[[], None]:
            self._assert_function_compatible(func)

            self._event_functions[event_type] = func
            return func

        return _on_event_decorator

    def on_shutdown(
        self: "App",
        func: Callable[[], None],
    ) -> Callable[[], None]:
        """
        Registers a callback function to be called when the application shuts down.

        Args:
            func (Callable[[], None]): The callback function to register.

        Returns:
            Callable[[], None]: The registered callback function.

        Raises:
            FunctionTypeError: If the function is not compatible with the selected concurrency mode.
        """

        return self.on_event(AppEventType.SHUTDOWN)(func)

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
            FunctionTypeError: If the function is not compatible with the selected concurrency mode.
        """

        return self.on_event(AppEventType.START)(func)

    def queue(
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self: "App",
        name: str,
        *,
        codec: Optional[CodecType | str] = None,
        forward_to: Optional[ForwardTarget] = None,
        persistent: bool = True,
        auto_delete: bool = False,
        retry_strategy: Optional[RetryStrategy] = None,
        dead_letter: Optional[ForwardTarget] = None,
        on_decode_error: Optional[EndpointExceptionHandler] = None,
        on_validation_error: Optional[EndpointExceptionHandler] = None,
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

        if self._started:
            raise RuntimeError("Cannot register endpoints after the app has started")

        codec = CodecType(codec) if codec is not None else self._codec_type

        # if the user didn't provide an explicit retry strategy, use the default
        # strategy that never performs retries
        if retry_strategy is None:
            retry_strategy = NoRetryStrategy()

        def _queue_decorator(func: Callable) -> QueueEndpoint:
            # check that the function is compatible with the selected concurrency mode
            self._assert_function_compatible(func)

            self._endpoints.append(
                EndpointFactory.create_queue_endpoint(
                    QueueEndpointConfig(
                        queue=Queue(
                            name=name,
                            persistent=persistent,
                            auto_delete=auto_delete,
                        ),
                        target=func,
                        codec_type=codec,
                        forward_to=forward_to,
                        retry_strategy=retry_strategy,
                        dead_letter=dead_letter,
                        error_handlers=QueueEndpointConfig.make_error_handlers_dict(
                            on_decode_error=on_decode_error,
                            on_validation_error=on_validation_error,
                        ),
                    )
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

        self._started = True

        # ensure the app is not already running
        if self._coordinator is not None:
            raise RuntimeError("App is already running")

        # instantiate the appropriate coordinator based on the concurrency mode
        if self.concurrency_mode == ConcurrencyMode.THREAD:
            self._init_threaded(engine)
        else:  # pragma: no cover
            raise NotImplementedError(
                f"Unimplemented concurrency mode '{self.concurrency_mode.value}'"
            )

        # validate all forward targets and emit warnings for any misconfigurations
        # before starting the coordinator
        self._validate_forward_targets()

        # perform any declarations before starting
        self.create_resources(engine)

        # run the coordinator
        assert self._coordinator is not None
        self._handle_event(AppEventType.START)
        self.logger.info(
            "Starting coordinator %s",
            type(self._coordinator).__name__,
        )
        self._coordinator.run()

        # when the coordinator is stopped by an interrupt, trigger the shutdown event
        self.logger.info("Coordinator has stopped, sending shutdown event")
        self._handle_event(AppEventType.SHUTDOWN)

    def _init_threaded(self: "App", engine: Engine) -> None:
        assert self.concurrency_mode == ConcurrencyMode.THREAD

        if self._coordinator is not None:  # pragma: no cover
            raise RuntimeError("App is already running")

        self._coordinator = ThreadCoordinator(
            endpoints=self._endpoints,
            engine=engine,
        )

    @property
    def started(self: "App") -> bool:
        """
        Property that indicates whether the application has been started.

        Returns:
            bool: True if the application has been started, False otherwise.
        """

        return self._started

    def stop(self: "App") -> None:
        """
        Stops the application by stopping the coordinator.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: If the application is not running.
        """

        if self._coordinator is None:
            raise RuntimeError("App is not running")

        self._coordinator.stop()

    def _validate_forward_targets(self: "App") -> None:
        # derive a set of names for queue forward targets and for queues that correspond
        # to a handler
        forward_destination_queue_names: Dict[Endpoint, str] = {}
        handler_queue_names: Set[str] = set()

        for endpoint in self._endpoints:
            # ignore any endpoints that aren't pointing at a queue
            if not isinstance(endpoint, QueueEndpoint):  # pragma: no cover
                raise NotImplementedError(
                    f"{type(endpoint)} not implemented for forward target validation"
                )
            handler_queue_names.add(endpoint.queue_name)

            # don't try getting info from endpoints that don't have a forward target that's a queue
            if endpoint.forward_target is None or not isinstance(
                endpoint.forward_target.resource, Queue
            ):
                continue
            forward_destination_queue_names[endpoint] = (
                endpoint.forward_target.resource.name
            )

        for (
            source_endpoint,
            target_queue_name,
        ) in forward_destination_queue_names.items():
            if target_queue_name not in handler_queue_names:
                warnings.warn(
                    f"Endpoint {source_endpoint.target.__qualname__}() forwards to queue "
                    f"'{target_queue_name}' but no handler is registered for that queue",
                    UnboundQueueWarning,
                    stacklevel=2,
                )
