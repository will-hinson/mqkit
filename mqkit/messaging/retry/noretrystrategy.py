"""
module mqkit.messaging.retry.noretrystrategy

This module implements a retry strategy that never retries a failed message.
This is the default behavior if a retry strategy is not specified for a
message handler.
"""

from typing import override

from .retrycontext import RetryContext
from .retrystrategy import RetryStrategy


class NoRetryStrategy(RetryStrategy):
    """
    class NoRetryStrategy

    This retry strategy never retries a failed message. This is the default
    behavior if a retry strategy is not specified for a message handler.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self: "NoRetryStrategy") -> None:
        super().__init__()

    @override
    def handle_failure(self: "NoRetryStrategy", context: RetryContext) -> None:
        self._forward_to_dlq(context)

        # simply acknowledge failure on the existing connection. we never
        # want to retry with this strategy
        context.connection.acknowledge_failure(context.message)
