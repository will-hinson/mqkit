"""
module mqkit.messaging.forwardtarget

Defines the ForwardTarget type, which represents a target for message forwarding. A ForwardTarget
can be a string (representing a queue name), a Queue object, an Exchange object, or a Destination
object. Apps and @consume marshal this internally to a Destination for forwarding messages.
"""

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..messaging import Destination, Exchange, Queue

ForwardTarget = Union[str, "Queue", "Exchange", "Destination"]
