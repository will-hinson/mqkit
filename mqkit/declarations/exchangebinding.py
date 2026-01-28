"""
module mqkit.declarations.exchangebinding

Defines the ExchangeBinding class representing a binding between an exchange or queue and
a topic (or routing key in RabbitMQ)
"""

from pydantic import BaseModel

from ..messaging import Exchange, Queue


class ExchangeBinding(BaseModel):
    """
    class ExchangeBinding

    Represents a binding between an exchange or queue and a topic.
    """

    bound_resource: Exchange | Queue
    topic: str
