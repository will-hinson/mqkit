from .marshalerror import MarshalError


class SerializeError(MarshalError):
    """Raised when an error occurs during serialization of a message."""
