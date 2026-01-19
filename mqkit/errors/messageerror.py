"""
module mqkit.errors.messageerror

Module defining the MessageError exception for message processing errors.
"""

from .mqkiterror import MqkitError


class MessageError(MqkitError):
    """Exception indicating an error related to message processing."""
