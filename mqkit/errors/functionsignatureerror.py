"""
module mqkit.errors.functionsignatureerror

Defines the FunctionSignatureError exception indicating a signature verification
error for a function.
"""

from .endpointerror import EndpointError


class FunctionSignatureError(EndpointError):
    """Raised when there is a signature verification error for a function."""
