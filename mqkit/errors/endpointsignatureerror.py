"""
module mqkit.errors.endpointsignatureerror

Defines the EndpointSignatureError exception indicating a signature verification
error for an endpoint.
"""

from .endpointerror import EndpointError


class EndpointSignatureError(EndpointError):
    """Raised when there is a signature verification error for an endpoint."""
