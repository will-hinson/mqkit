"""
module mqkit.errors.decodeerror

Defines the DecodeError exception indicating an error during decoding
of a message.
"""

from .marshalerror import MarshalError


class DecodeError(MarshalError):
    """Raised when an error occurs during decoding of a message."""
