"""
module mqkit.errors.shutdownrequested

Defines the ShutdownRequested exception indicating that a shutdown has been requested
for a message queue worker or application.
"""

from .mqkiterror import MqkitError


class ShutdownRequested(MqkitError):
    """Exception indicating that a shutdown has been requested."""
