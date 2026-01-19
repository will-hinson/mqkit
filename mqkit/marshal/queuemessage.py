"""
module mqkit.marshal.queuemessage

Defines the QueueMessage data model representing a message in a message queue.
"""

from typing import Any, Dict

from pydantic import BaseModel


class QueueMessage(BaseModel):
    """
    class QueueMessage

    Represents a message in a message queue, including its data and attributes.
    """

    data: bytes
    attributes: Dict[str, Any]
