from .mqkiterror import MqkitError


class ShutdownRequested(MqkitError):
    """Exception indicating that a shutdown has been requested."""
