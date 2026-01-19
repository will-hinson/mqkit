"""
module mqkit.apps.concurrencymode

Defines the ConcurrencyMode enumeration for specifying concurrency modes
in message queue applications.
"""

from enum import Enum


class ConcurrencyMode(str, Enum):
    """
    class ConcurrencyMode

    An enumeration representing the concurrency modes available for message
    queue applications.
    """

    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"
