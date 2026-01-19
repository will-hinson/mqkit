"""
module mqkit.errors.serializeerror

Defines the SerializeError exception indicating an error during serialization
of a message.
"""

from .marshalerror import MarshalError


class SerializeError(MarshalError):
    """Raised when an error occurs during serialization of a message."""
