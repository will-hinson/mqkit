from typing import Callable, Optional

from .endpoint import Endpoint
from ..marshal import Forward, QueueMessage
from ..marshal.codecs import CodecType


class QueueEndpoint(Endpoint):
    _forward_to: Optional[str] = None

    def __init__(
        self: "QueueEndpoint",
        queue_name: str,
        target: Callable,
        codec_type: CodecType,
        forward_to: Optional[str] = None,
    ) -> None:
        super().__init__(
            queue_name=queue_name,
            target=target,
            codec_type=codec_type,
        )

        self._forward_to = forward_to

    def _forward_result(self: "QueueEndpoint", data: bytes) -> Optional[Forward]:
        assert self._forward_to is not None

        if isinstance(self._forward_to, str):
            return Forward(
                forward_target=self._forward_to,
                message=QueueMessage(
                    data=data,
                    attributes={
                        "x-mqkit-forwarded": "true",
                        "x-mqkit-origin-queue": self._queue_name,
                    },
                ),
            )
        else:
            raise NotImplementedError(
                "Forwarding to non-str targets is not implemented"
            )

    def handle_message(
        self: "QueueEndpoint", message: QueueMessage
    ) -> Optional[Forward]:
        result: Optional[bytes] = self.target(
            message=message.data,
            attributes=message.attributes,
        )

        if result is None:
            return

        # check if the message can actually be replied to
        if self._forward_to is None:
            raise ValueError(
                "Cannot forward returned message result because no forward_to queue was specified"
            )

        return self._forward_result(result)

    @property
    def qualname(self: "QueueEndpoint") -> str:
        return self._queue_name
