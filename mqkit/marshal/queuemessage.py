"""
module mqkit.marshal.queuemessage

Defines the QueueMessage data model representing a message in a message queue.
"""

from pydantic import BaseModel

from .attributes import Attributes


class QueueMessage(BaseModel):
    """
    class QueueMessage

    Represents a message in a message queue, including its data and attributes.
    """

    data: bytes
    attributes: Attributes
