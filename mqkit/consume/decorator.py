"""
module mqkit.consume.decorator

Defines the consume decorator for designating functions as message queue consumers.
This is a single-consumer alternative to the mqkit.apps.App class.
"""

import asyncio
from logging import Logger
import logging
import os
import sys
from typing import Callable, NoReturn, Optional, Union

from ..engines import create_engine, Engine
from ..endpoints import EndpointFactory
from ..endpoints.config import QueueEndpointConfig
from ..marshal.codecs import CodecType
from ..messaging import Exchange, Queue
from ..workers.threaded import ThreadWorker


def _consume_threaded(
    config: QueueEndpointConfig,
    engine: Engine,
    logger: Logger,
) -> None:
    # instantiate the endpoint and start a threaded worker
    worker: ThreadWorker = ThreadWorker(
        EndpointFactory.create_queue_endpoint(config),
        engine=engine,
        error_queue=None,
    )
    try:
        worker.start()
        logger.info("Started threaded consumer for queue '%s'", config.queue.name)
        worker.join()
    except KeyboardInterrupt:  # pragma: no cover
        worker.stop("Keyboard interrupt received")
        worker.join()

    # if an exception occurred in the worker, re-raise it here
    if worker.error is not None:
        logger.exception(
            "Worker for queue '%s' exited with exception: %s",
            config.queue.name,
            worker.error,
            exc_info=worker.error,
        )
        raise worker.error


def _infer_engine() -> Engine:
    # try to infer the engine from an environment variable
    if "MQKIT_ENGINE_URL" not in os.environ:
        raise RuntimeError(
            "No engine provided and MQKIT_ENGINE_URL environment variable not set"
        )

    return create_engine(os.environ["MQKIT_ENGINE_URL"])


def _infer_logger(func: Callable) -> Logger:
    return logging.getLogger(
        os.environ.get(
            "MQKIT_CONSUMER_LOGGER_NAME",
            func.__name__,
        )
    )


def consume(
    # pylint: disable=too-many-arguments,duplicate-code
    name: str,
    *,
    engine: Optional[Engine] = None,
    logger: Optional[Logger] = None,
    codec: Optional[CodecType | str] = None,
    forward_to: Optional[Union[str, Queue, Exchange]] = None,
    persistent: bool = True,
    auto_delete: bool = False,
) -> Callable[[Callable], NoReturn]:
    """
    Blocking decorator to designate a callback function as a consumer for a single message queue.
    Intended for use in applications that do not require single-process concurrency (for example,
    within a scaling container like a Kubernetes pod).

    Args:
        name (str): The name of the queue.
        codec (Optional[CodecType | str]): The codec type for message serialization.
            Defaults to CodecType.JSON.
        forward_to (Optional[str]): The name of the queue to forward results to.
            Defaults to None.
        persistent (bool): Whether the queue is persistent. Defaults to True.
        auto_delete (bool): Whether the queue is auto-delete. Defaults to False.

    Returns:
        Callable[[Callable], QueueEndpoint]: The decorator function.

    Raises:
        TypeError: If the function is not compatible with the selected concurrency mode.
    """

    if engine is None:
        engine = _infer_engine()

    codec = CodecType(codec) if codec is not None else CodecType.JSON

    def _consume_decorator(func: Callable) -> NoReturn:
        # if a logger was not provided, infer one from the environment
        nonlocal logger
        if logger is None:
            logger = _infer_logger(func)

        # NOTE: eventually add async functionality here
        if asyncio.iscoroutinefunction(func):
            raise NotImplementedError(
                "Async functions are not supported by the @consume decorator"
            )

        # run in threaded mode. exit with code 0 if no exception raised (should only
        # exit on KeyboardInterrupt)
        _consume_threaded(
            config=QueueEndpointConfig(
                queue=Queue(
                    name=name,
                    persistent=persistent,
                    auto_delete=auto_delete,
                ),
                target=func,
                codec_type=codec,
                forward_to=forward_to,
            ),
            engine=engine,
            logger=logger,
        )
        sys.exit(0)

    return _consume_decorator
