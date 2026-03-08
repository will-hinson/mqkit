"""
module mqkit.errors.configurationerror

This module defines the ConfigurationError class, which represents an error that occurs when
there is a problem with the configuration of an app/consumer. This could include issues such
as invalid parameters, missing required settings, or incompatible configurations. The
ConfigurationError class provides a way to signal that there is a problem with the
configuration and to provide details about the specific issue that occurred.
"""

from .mqkiterror import MqkitError


class ConfigurationError(MqkitError):
    """
    class ConfigurationError

    Represents an error that occurs when there is a problem with the configuration of the system.
    This could include issues such as invalid parameters, missing required settings, or incompatible
    configurations. The ConfigurationError class provides a way to signal that there is a problem
    with the configuration and to provide details about the specific issue that occurred.
    """
