"""
module mqkit.errors.apperror

Defines the AppError exception indicating an application-level error.
"""

from .mqkiterror import MqkitError


class AppError(MqkitError):
    """Exception indicating an application-level error."""
