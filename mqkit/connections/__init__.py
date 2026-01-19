"""
module mqkit.connections

Defines connection types and utilities for message queue connections.
Intended for internal use within the mqkit library by the App class
to manage different connection protocols and configurations.
"""

__all__ = ["amqp", "Connection"]

from . import amqp

from .connection import Connection
