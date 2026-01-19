"""
module mqkit.errors

Defines various exception classes used in the MQKit library.
"""

__all__ = [
    "MarshalError",
    "MqkitError",
    "NoRetry",
    "SerializeError",
    "ShutdownRequested",
]

from .marshalerror import MarshalError
from .mqkiterror import MqkitError
from .noretry import NoRetry
from .serializeerror import SerializeError
from .shutdownrequested import ShutdownRequested
