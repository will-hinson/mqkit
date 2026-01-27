"""
module mqkit.errors.workerterminatederror

Defines the WorkerTerminatedError exception for handling unexpected worker terminations.
"""

from .mqkiterror import MqkitError


class WorkerTerminatedError(MqkitError):
    """Raised when a worker process has been terminated unexpectedly."""
