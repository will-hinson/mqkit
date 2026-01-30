"""
module mqkit.messaging.destination

Defines the Destination class representing a messaging destination.
"""

from typing import Optional, Union

from pydantic import BaseModel

from .exchange import Exchange
from .queue import Queue


class Destination(BaseModel):
    """
    class Destination

    Represents a messaging destination, which can be either a Queue or an Exchange.
    """

    resource: Union[Queue, Exchange]
    topic: Optional[str] = None
