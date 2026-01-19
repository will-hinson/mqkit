"""
module mqkit.errors.functiontypeerror

Module defining the FunctionTypeError exception for MQKit.
"""

from .apperror import AppError


class FunctionTypeError(AppError):
    """Exception indicating that a callback function has an incorrect type."""
