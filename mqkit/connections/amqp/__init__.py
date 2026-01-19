"""
module mqkit.connections.amqp

A module providing AMQP connection and consumption thread classes for message queue applications.
"""

__all__ = [
    "AmqpConnection",
    "AmqpConsumeThread",
]

from .amqpconnection import AmqpConnection
from .amqpconsumethread import AmqpConsumeThread
