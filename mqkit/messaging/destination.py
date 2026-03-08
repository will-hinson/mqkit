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

    @classmethod
    def from_forward_target(
        cls,
        forward_to: Optional[Union[str, Queue, Exchange, "Destination"]] = None,
    ) -> Optional["Destination"]:
        """
        Converts a forward target to a Destination object.

        Args:
            forward_to (Optional[Union[str, Queue, Exchange, Destination]]): The forward target
                to convert. Can be a string (queue name), a Queue, an Exchange, or a Destination.

        Returns:
            Optional[Destination]: The converted Destination object, or None if the input is None.
        """

        if forward_to is None:
            return None

        if isinstance(forward_to, str):
            forward_to = Queue(name=forward_to)

        if isinstance(forward_to, (Queue, Exchange)):
            return cls(resource=forward_to)

        return forward_to
