"""
module mqkit.endpoints.queueendpoint

Defines the QueueEndpoint class for processing messages from a message queue.
"""

from copy import copy
from typing import Optional, override

from .config import QueueEndpointConfig
from .endpoint import Endpoint
from ..errors import NoForwardTargetError
from ..messaging import Attributes, Forward, Queue, QueueMessage


class QueueEndpoint(Endpoint):
    """
    class QueueEndpoint

    Represents an endpoint for processing messages from a message queue.
    """

    _config: QueueEndpointConfig

    def __init__(
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self: "QueueEndpoint",
        config: QueueEndpointConfig,
    ) -> None:
        super().__init__(
            target=config.target,
            codec_type=config.codec_type,
        )

        self._config = config

    def _forward_result(self: "QueueEndpoint", data: bytes) -> Optional[Forward]:
        assert self._config.forward_to is not None

        if isinstance(self._config.forward_to, Queue):
            return Forward(
                forward_target=self._config.forward_to,
                message=QueueMessage(
                    data=data,
                    attributes=Attributes(
                        headers={
                            "x-mqkit-forwarded": "true",
                            "x-mqkit-origin-queue": self._config.queue.name,
                        },
                        forwarded=True,
                        origin_queue=self._config.queue.name,
                        topic=None,
                    ),
                ),
            )

        raise NotImplementedError(
            "Forwarding to non-Queue targets is not implemented"
        )  # pragma: no cover

    def handle_message(
        self: "QueueEndpoint", message: QueueMessage
    ) -> Optional[Forward]:
        """
        Handle an incoming message by invoking the target function.

        Args:
            message (QueueMessage): The incoming message to handle.

        Returns:
            Optional[Forward]: A Forward object if the result should be forwarded,
                otherwise None.

        Raises:
            ValueError: If the target function returns a result but no forward_to
                queue was specified.
        """

        result: Optional[bytes] = self.target(
            message=message.data,
            attributes=message.attributes,
        )

        if result is None:
            return None

        # check if the message can actually be replied to
        if self._config.forward_to is None:
            raise NoForwardTargetError(
                "Cannot forward returned message result because no forward_to queue was specified"
            )

        return self._forward_result(result)

    @property
    @override
    def is_auto_delete(self: "QueueEndpoint") -> bool:
        return self._config.queue.auto_delete

    @property
    @override
    def is_persistent(self: "QueueEndpoint") -> bool:
        return self._config.queue.persistent

    @property
    def qualname(self: "QueueEndpoint") -> str:  # pragma: no cover
        return self.queue_name

    @property
    def queue_name(self: "QueueEndpoint") -> str:
        return copy(self._config.queue.name)
