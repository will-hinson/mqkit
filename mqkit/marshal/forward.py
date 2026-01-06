from pydantic import BaseModel

from .queuemessage import QueueMessage


class Forward(BaseModel):
    forward_target: str
    message: QueueMessage
