from .endpoint import Endpoint
from ..marshal import QueueMessage


class QueueEndpoint(Endpoint):
    def handle_message(self: "QueueEndpoint", message: QueueMessage) -> None:
        print(
            self.target(
                message=message.data,
                attributes=message.attributes,
            )
        )

    @property
    def qualname(self: "QueueEndpoint") -> str:
        return self._queue_name
