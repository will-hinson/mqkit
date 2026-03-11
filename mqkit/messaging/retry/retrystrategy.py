"""
module mqkit.messaging.retry.retrystrategy

This module defines the base class for retry strategies. A retry strategy is responsible for
handling failed messages and determining whether to retry them or not. Subclasses of
RetryStrategy can implement different retry policies
"""

from abc import ABCMeta, abstractmethod
from logging import Logger
import logging
import traceback

from ..exceptionhistoryentry import ExceptionHistoryEntry
from ..forward import Forward
from ...logging import root_logger_name
from .retrycontext import RetryContext


class RetryStrategy(metaclass=ABCMeta):
    """
    class RetryStrategy

    This is the base class for retry strategies. A retry strategy is responsible for handling
    failed messages and determining whether to retry them or not. Subclasses of RetryStrategy
    can implement different retry policies.
    """

    # pylint: disable=too-few-public-methods

    _logger: Logger

    def __init__(self: "RetryStrategy") -> None:
        self._logger = logging.getLogger(
            f"{root_logger_name}.{'.'.join(self.__class__.__module__.split('.')[1:-1])}."
            f"{self.__class__.__name__}"
        )

    def _append_exception_to_history(
        self: "RetryStrategy", context: RetryContext
    ) -> None:
        # append an object representing the current exception to the history
        context.message.attributes.exception_history.append(
            ExceptionHistoryEntry(
                exception_type=type(context.exception).__qualname__,
                exception_module=type(context.exception).__module__,
                exception_message=str(context.exception),
                traceback=traceback.format_exception(context.exception),
                retry_count=context.message.attributes.retry_count,
                origin_queue=context.message.attributes.origin_queue,
            )
        )

    def _forward_to_dlq(self: "RetryStrategy", context: RetryContext) -> None:
        if context.dead_letter_destination is None:
            self._logger.warning(
                "No dead letter destination configured, message will be discarded"
            )
        else:
            self._logger.info(
                "Forwarding failed message to dead letter destination: %s",
                context.dead_letter_destination,
            )

            # set up the message with the appropriate dlq context
            if context.dead_letter_destination.topic is not None:
                context.message.attributes.topic = context.dead_letter_destination.topic
            context.message.attributes.headers["x-mqkit-previous-retry-count"] = str(
                context.message.attributes.retry_count
            )
            context.message.attributes.headers |= {
                "x-mqkit-forwarded": "true",
                "x-mqkit-dead-letter": "true",
                "x-mqkit-origin-queue": context.received_queue,
            }
            self._append_exception_to_history(context)
            context.message.attributes.retry_count = 0

            context.connection.forward_message(
                Forward(
                    forward_target=context.dead_letter_destination.resource,
                    message=context.message,
                )
            )

    @abstractmethod
    def handle_failure(
        self: "RetryStrategy", context: RetryContext
    ) -> None:  # pragma: no cover
        """
        This method is called when a message handler fails to process a message. The retry strategy
        is responsible for handling the failure and determining whether to retry the message or not.

        Args:
            context (RetryContext): The context of the failed message, including the message itself,
            the connection, and any relevant metadata.

        Returns:
            None

        Raises:
            Exception: If the retry strategy encounters an error while handling the failure, it
                should raise an exception to indicate that the failure could not be handled.
        """

        raise NotImplementedError("handle_retry must be implemented by subclasses")
