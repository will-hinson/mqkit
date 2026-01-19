"""
module mqkit.errors.marshalerror

Defines the MarshalError exception indicating errors during marshalling or unmarshalling data.
"""

from .mqkiterror import MqkitError


class MarshalError(MqkitError):
    """Exception raised for errors during marshalling or unmarshalling data."""
