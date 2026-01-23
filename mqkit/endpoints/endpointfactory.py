"""
module mqkit.endpoints.endpointfactory

Defines the EndpointFactory class for creating endpoint instances.
"""

from .config import QueueEndpointConfig
from .queueendpoint import QueueEndpoint


class EndpointFactory:
    """
    class EndpointFactory

    Factory class for creating endpoint instances.
    """

    # pylint: disable=too-few-public-methods

    @staticmethod
    def create_queue_endpoint(config: QueueEndpointConfig) -> QueueEndpoint:
        """
        Creates a QueueEndpoint instance based on the provided configuration.

        Args:
            config (QueueEndpointConfig): The configuration for the queue endpoint.

        Returns:
            QueueEndpoint: The created QueueEndpoint instance.
        """

        return QueueEndpoint(config)
