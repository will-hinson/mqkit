"""
module mqkit.endpoints.endpointfactory

Defines the EndpointFactory class for creating endpoint instances.
"""

from typing import TYPE_CHECKING, Optional

from ..messaging import Destination
from .queueendpoint import QueueEndpoint

if TYPE_CHECKING:
    from .config import QueueEndpointConfig
    from ..messaging import ForwardTarget


class EndpointFactory:
    """
    class EndpointFactory

    Factory class for creating endpoint instances.
    """

    # pylint: disable=too-few-public-methods

    @staticmethod
    def create_queue_endpoint(config: "QueueEndpointConfig") -> QueueEndpoint:
        """
        Creates a QueueEndpoint instance based on the provided configuration.

        Args:
            config (QueueEndpointConfig): The configuration for the queue endpoint.

        Returns:
            QueueEndpoint: The created QueueEndpoint instance.
        """

        return QueueEndpoint(config)

    @staticmethod
    def convert_forward_target_to_destination(
        forward_to: Optional["ForwardTarget"] = None,
    ) -> Optional[Destination]:
        """
        Converts a forward target to a Destination object.

        Args:
            forward_to (Optional[Union[str, Queue, Exchange, Destination]]): The forward target
                to convert.

        Returns:
            Optional[Destination]: The converted Destination object, or None if the input is None.
        """

        return Destination.from_forward_target(forward_to)
