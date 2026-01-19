"""
module mqkit.marshal.codecs.codec

Defines the abstract base class Codec for encoding and decoding data.
"""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict


class Codec(metaclass=ABCMeta):
    """
    class Codec

    An abstract base class for codecs that encode and decode data.
    """

    @abstractmethod
    def decode(self: "Codec", data: bytes) -> Dict[str, Any]:
        """
        Decode the given bytes data into a dictionary.

        Args:
            data (bytes): The bytes data to decode.

        Returns:
            Dict[str, Any]: The decoded data as a dictionary.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def encode(self: "Codec", data: Dict[str, Any]) -> bytes:
        """
        Encode the given dictionary data into bytes.

        Args:
            data (Dict[str, Any]): The dictionary data to encode.

        Returns:
            bytes: The encoded data as bytes.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()
