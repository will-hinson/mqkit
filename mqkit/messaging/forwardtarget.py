from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..messaging import Destination, Exchange, Queue

ForwardTarget = Union[str, "Queue", "Exchange", "Destination"]
