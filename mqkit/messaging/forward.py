"""
module mqkit.marshal.forward

A module defining the Forward data model for message forwarding.
"""

from typing import Union

from pydantic import BaseModel

from .exchange import Exchange
from .queue import Queue
from .queuemessage import QueueMessage


class Forward(BaseModel):
    """
    class Forward

    Represents a forwarding instruction for a message, specifying the target
    queue and the message to be forwarded.
    """

    forward_target: Union[Queue, Exchange]
    message: QueueMessage
