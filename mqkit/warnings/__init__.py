"""
module mqkit.warnings

This module defines the warnings that are used in the mqkit package. These warnings are used to
inform users of potential issues with their configuration or usage of the package. The warnings
defined in this module include:
    - ConfigurationWarning: A warning that is emitted when a user has a potential issue with their
        configuration.
    - MqkitWarning: Base class for all mqkit warnings
    - UnboundQueueWarning: A warning that is emitted when a user defines a forward queue for an
        endpoint that doesn't have a handler defined.
"""

__all__ = [
    "ConfigurationWarning",
    "MqkitWarning",
    "UnboundQueueWarning",
]

from .configurationwarning import ConfigurationWarning
from .mqkitwarning import MqkitWarning
from .unboundqueuewarning import UnboundQueueWarning
