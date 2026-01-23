"""
module mqkit.marshal.codecs.rawcodec

Defines the RawCodec for handling raw byte payloads.
"""

from .codec import Codec


class RawCodec(Codec):
    """
    class RawCodec

    A codec for handling raw byte payloads without any serialization or deserialization.
    """

    @property
    def content_type(self: "RawCodec") -> str:
        return "application/octet-stream"

    def decode(self: "RawCodec", data: bytes) -> bytes:
        return data

    def encode(self: "RawCodec", data: bytes) -> bytes:
        return data
