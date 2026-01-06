from .mqkiterror import MqkitError


class NoRetry(MqkitError):
    """Exception indicating that the operation should not be retried."""
