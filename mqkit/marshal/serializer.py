"""
module mqkit.marshal.serializer

Defines the abstract base class Serializer for serializing and deserializing data
to and from bytestrings using a specified codec.
"""

from abc import ABCMeta, abstractmethod
from typing import Any, Optional

from .codecs import Codec


class Serializer(metaclass=ABCMeta):
    """
    class Serializer

    Abstract base class for serializers that convert data to and from bytestrings
    using a specific codec.
    """

    _codec: Codec

    def __init__(self: "Serializer", codec: Codec) -> None:
        self._codec = codec

    @abstractmethod
    def deserialize(self: "Serializer", data: bytes) -> Any:
        """
        Deserialize the given bytestring into data. The returned data should be of
        the appropriate type for the codec associated with this serializer.

        Args:
            data (bytes): The bytestring to deserialize.

        Returns:
            Any: The deserialized data.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def serialize(self: "Serializer", data: Any) -> Optional[bytes]:
        """
        Serialize the given data into a bytestring. Data should be of the
        appropriate type for the codec associated with this serializer.

        Args:
            data (Any): The data to serialize.

        Returns:
            Optional[bytes]: The serialized data as a bytestring, or None if
                the data is None.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()
