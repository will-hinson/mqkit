"""
module mqkit.marshal.forward

A module defining the Forward data model for message forwarding.
"""

from pydantic import BaseModel

from .queue import Queue
from .queuemessage import QueueMessage


class Forward(BaseModel):
    """
    class Forward

    Represents a forwarding instruction for a message, specifying the target
    queue and the message to be forwarded.
    """

    forward_target: Queue
    message: QueueMessage
