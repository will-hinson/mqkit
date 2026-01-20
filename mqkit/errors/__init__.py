"""
module mqkit.errors

Defines various exception classes used in the MQKit library.
"""

__all__ = [
    "AppError",
    "DecodeError",
    "EncodeError",
    "EndpointError",
    "FunctionSignatureError",
    "FunctionTypeError",
    "MarshalError",
    "MessageError",
    "MqkitError",
    "NoForwardTargetError",
    "NoRetry",
    "SerializeError",
    "ShutdownRequested",
]

from .apperror import AppError
from .decodeerror import DecodeError
from .encodeerror import EncodeError
from .endpointerror import EndpointError
from .functionsignatureerror import FunctionSignatureError
from .functiontypeerror import FunctionTypeError
from .marshalerror import MarshalError
from .messageerror import MessageError
from .mqkiterror import MqkitError
from .noforwardtargeterror import NoForwardTargetError
from .noretry import NoRetry
from .serializeerror import SerializeError
from .shutdownrequested import ShutdownRequested
