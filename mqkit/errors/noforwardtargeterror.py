"""
module mqkit.errors.noforwardtargeterror

Defines the NoForwardTargetError exception indicating that a message
"""

from .messageerror import MessageError


class NoForwardTargetError(MessageError):
    """Raised when a message has no forward target defined."""
