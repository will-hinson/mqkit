"""
module mqkit.messaging.retry.retrystrategy

This module defines the base class for retry strategies. A retry strategy is responsible for
handling failed messages and determining whether to retry them or not. Subclasses of
RetryStrategy can implement different retry policies
"""

from abc import ABCMeta, abstractmethod
from logging import Logger
import logging

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
