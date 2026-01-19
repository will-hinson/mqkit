"""
module mqkit.connections.amqp.amqpsentinel

Defines the AmqpSentinel exception used as a sentinel value in AMQP connections.
An instance of this exception can be passed into the internal Queue to signal
that the connection should be shut down
"""

from ...errors import MqkitError


class AmqpSentinel(MqkitError):
    """
    Sentinel exception used to signal shutdown in AMQP connections.
    """
