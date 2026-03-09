"""
module mqkit.messaging.retry.noretrystrategy

This module implements a retry strategy that never retries a failed message.
This is the default behavior if a retry strategy is not specified for a
message handler.
"""

from typing import override

from ..forward import Forward
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
        if context.dead_letter_destination is not None:
            self._logger.info(
                "Message failed, forwarding to dead letter destination %s. Will not retry",
                context.dead_letter_destination,
            )
            context.connection.forward_message(
                Forward(
                    forward_target=context.dead_letter_destination.resource,
                    message=context.message,
                )
            )
        else:
            self._logger.info("Acknowledged failure of message, will not retry")

        # simply acknowledge failure on the existing connection. we never
        # want to retry with this strategy
        context.connection.acknowledge_failure(context.message)
