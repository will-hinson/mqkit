"""
module mqkit.errors.endpointerror

Defines the EndpointError exception indicating an error related to an endpoint
configuration or operation.
"""

from .mqkiterror import MqkitError


class EndpointError(MqkitError):
    """Exception indicating an error related to an endpoint configuration or operation."""
