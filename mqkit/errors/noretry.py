"""
module mqkit.errors.noretry

Defines the NoRetry exception indicating that the operation should not be retried.
This is intended to be used by user code to signal that a handler for a queue
message should not be retried due to some condition.
"""

from .mqkiterror import MqkitError


class NoRetry(MqkitError):
    """Exception indicating that the operation should not be retried."""
