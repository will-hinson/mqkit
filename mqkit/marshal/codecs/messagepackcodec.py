"""
module mqkit.marshal.codecs.messagepackcodec

Defines the MessagePackCodec class for encoding and decoding messages using
the MessagePack format.
"""

from typing import Any, Dict

import msgpack

from .codec import Codec
from ...errors import DecodeError, EncodeError


class MessagePackCodec(Codec):
    """
    class MessagePackCodec

    Codec for encoding and decoding messages using MessagePack format
    """

    @property
    def content_type(self: "MessagePackCodec") -> str:
        return "application/msgpack"

    def encode(self, data: Dict[str, Any]) -> bytes:
        """
        Encodes a message dictionary into MessagePack bytes.

        Args:
            message (Dict[str, Any]): The message to encode.

        Returns:
            bytes: The encoded MessagePack bytes.
        """

        result: bytes | None = self._pack(data)

        if result is None:
            raise DecodeError("Failed to encode data to MessagePack (value was None)")

        return result

    def decode(self, data: bytes) -> Dict[str, Any]:
        """
        Decodes MessagePack bytes into a message dictionary.

        Args:
            data (bytes): The MessagePack bytes to decode.

        Returns:
            Dict[str, Any]: The decoded message.

        Raises:
            DecodeError: If decoding fails.
        """

        result: Any = self._unpack(data)

        if not isinstance(result, dict):
            raise DecodeError("Decoded MessagePack data is not a dictionary")

        return result

    def _pack(self: "MessagePackCodec", data: Any) -> bytes | None:
        try:
            return msgpack.packb(data, use_bin_type=True)
        except (TypeError, ValueError) as e:
            raise EncodeError("Failed to encode data to MessagePack") from e

    def _unpack(self: "MessagePackCodec", data: bytes) -> Any:
        try:
            return msgpack.unpackb(data, raw=False)
        except ValueError as ve:
            raise DecodeError("Failed to decode MessagePack data") from ve
