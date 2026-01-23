"""
module mqkit.marshal.queue

Defines the Queue model for message queue configurations.
"""

from pydantic import BaseModel


class Queue(BaseModel):
    """
    class Queue

    Model for a message queue configuration.
    """

    name: str
    persistent: bool = True
    auto_delete: bool = False
