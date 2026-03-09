"""
module mqkit.warnings.unboundqueuewarning

This module defines the UnboundQueueWarning class, which is a warning that is emitted when
a user defines a forward queue for an endpoint that doesn't have a handler defined.
"""

from .configurationwarning import ConfigurationWarning


class UnboundQueueWarning(ConfigurationWarning):
    """
    class UnboundQueueWarning

    Warning that is emitted when a user defines a forward queue for an endpoint
    that doesn't have a handler defined
    """
