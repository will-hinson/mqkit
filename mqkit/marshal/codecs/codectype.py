"""
module mqkit.marshal.codecs.codectype

Defines the CodecType enumeration for supported serialization formats.
"""

from enum import Enum


class CodecType(str, Enum):
    """
    class CodecType

    Enumeration of supported serialization formats for message payloads.
    """

    JSON = "json"
    MESSAGEPACK = "messagepack"
    YAML = "yaml"
