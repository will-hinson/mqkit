"""
module mqkit.declarations.queuedeclaration

Defines the QueueDeclaration model representing a queue declaration.
"""

from pydantic import BaseModel

from ..messaging import Queue


class QueueDeclaration(BaseModel):
    """
    class QueueDeclaration

    Model representing a queue declaration.
    """

    queue: Queue
