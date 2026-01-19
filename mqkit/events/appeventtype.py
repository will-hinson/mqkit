"""
module mqkit.events.appeventtype

Defines the AppEventType enumeration for application event types that
can result in callbacks being invoked.

The user can register callback functions for these event types using
decorators provided by the App class.
"""

from enum import Enum


class AppEventType(str, Enum):
    """
    class AppEventType

    An enumeration representing the different types of application events
    that can trigger callbacks.
    """

    START = "start"
