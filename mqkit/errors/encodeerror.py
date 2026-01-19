"""
module mqkit.errors.encodeerror

Defines the EncodeError exception indicating an error during encoding
of a message.
"""

from .marshalerror import MarshalError


class EncodeError(MarshalError):
    """Raised when an error occurs during encoding of a message."""
