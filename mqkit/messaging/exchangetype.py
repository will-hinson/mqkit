"""
module mqkit.messaging.exchangetype

Defines the ExchangeType enumeration for message queue exchanges.
"""

from enum import Enum


class ExchangeType(str, Enum):
    """
    class ExchangeType

    Enumeration of supported message queue exchange types.
    """

    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"
