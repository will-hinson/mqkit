import json
import traceback
from typing import List, Optional, override

from pydantic import ValidationError

from ..destination import Destination
from ...errors import MarshalError
from .exceptionhistoryentry import ExceptionHistoryEntry
from ..forwardtarget import ForwardTarget
from ...messaging import Forward
from .retrycontext import RetryContext
from .retrystrategy import RetryStrategy


class ImmediateRetryStrategy(RetryStrategy):
    # pylint: disable=too-few-public-methods

    _retries: int
    _dead_letter_destination: Optional[Destination]

    def __init__(
        self: "ImmediateRetryStrategy",
        retries: int,
        dead_letter_destination: Optional[ForwardTarget] = None,
    ) -> None:
        super().__init__()

        from ...endpoints import EndpointFactory

        self._retries = retries
        self._dead_letter_destination = (
            EndpointFactory.convert_forward_target_to_destination(
                dead_letter_destination
            )
        )

    @override
    def handle_failure(self: "ImmediateRetryStrategy", context: RetryContext) -> None:
        # first, check if the failure was due to a marshalling error. if so, we assume
        # the message was malformed and don't try to retry it
        if issubclass(type(context.exception), MarshalError):
            self._handle_failure_bad_message(context)
            return

        # then, check if we have already exceeded the maximum number of retries. if so,
        # acknowledge failure and do not retry
        if context.message.attributes.retry_count >= self._retries:
            self._handle_failure_retries_exceeded(context)
            return

        # otherwise, submit the message for retry in the origin queue. then, nack the
        # original message to remove it from the queue since we have requeued it
        self._submit_for_retry(context)
        context.connection.acknowledge_failure(context.message)

    def _append_exception_to_history(
        self: "ImmediateRetryStrategy", context: RetryContext
    ) -> None:
        # try decoding any existing exception history from the headers
        exception_history: List[ExceptionHistoryEntry] = []
        try:
            exception_history = [
                ExceptionHistoryEntry(**object)
                for object in json.loads(
                    context.message.attributes.headers.get(
                        "x-mqkit-exception-history", "[]"
                    )
                )
            ]
        except (json.JSONDecodeError, ValidationError) as e:
            self._logger.warning(
                "Failed to decode exception history from message headers, "
                "proceeding without empty history (%s: %s)",
                type(e),
                e,
            )

        # append an object representing the current exception to the history
        exception_history.append(
            ExceptionHistoryEntry(
                exception_type=type(context.exception).__qualname__,
                exception_message=str(context.exception),
                traceback=traceback.format_exception(context.exception),
                retry_count=context.message.attributes.retry_count,
            )
        )

        # re-encode the updated exception history back into the headers
        context.message.attributes.headers["x-mqkit-exception-history"] = json.dumps(
            [entry.model_dump() for entry in exception_history]
        )

    def _forward_to_dlq(self: "ImmediateRetryStrategy", context: RetryContext) -> None:
        if self._dead_letter_destination is None:
            self._logger.warning(
                "No dead letter destination configured, message will be discarded"
            )
        else:
            self._logger.info(
                f"Forwarding failed message to dead letter destination: "
                f"{self._dead_letter_destination}"
            )

            # set up the message with the appropriate dlq context
            if self._dead_letter_destination.topic is not None:
                context.message.attributes.topic = self._dead_letter_destination.topic
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
                    forward_target=self._dead_letter_destination.resource,
                    message=context.message,
                )
            )

    def _handle_failure_bad_message(
        self: "ImmediateRetryStrategy", context: RetryContext
    ) -> None:
        self._logger.error(
            f"Message handling failed with marshal error: {context.exception}. "
            "Will not retry due to malformed message data"
        )
        self._forward_to_dlq(context)
        context.connection.acknowledge_failure(context.message)

    def _handle_failure_retries_exceeded(
        self: "ImmediateRetryStrategy", context: RetryContext
    ) -> None:
        self._logger.info(
            f"Message handling failed with exception: {context.exception}. "
            f"Exceeded maximum retry count of {self._retries} (current retry count: "
            f"{context.message.attributes.retry_count}). Acknowledging failure and "
            "will not retry"
        )

        # forward a copy of the message to the dead letter destination if configured
        # then acknowledge failure on the original message to remove it from the queue
        self._forward_to_dlq(context)
        context.connection.acknowledge_failure(context.message)

    def _submit_for_retry(
        self: "ImmediateRetryStrategy", context: RetryContext
    ) -> None:
        # submit the message for retry by requeuing it with an incremented retry count in the headers
        self._append_exception_to_history(context)
        context.message.attributes.retry_count += 1
        context.connection.submit_message(context.message)
        self._logger.info(
            "Requeued message for immediate retry (current retry count: "
            f"{context.message.attributes.retry_count})"
        )
