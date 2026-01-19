"""
module mqkit.connections.amqp.amqpmessage

Defines the AmqpMessage data model representing an AMQP message with its associated
channel, method, properties, and body.
"""

from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method
from pydantic import BaseModel, ConfigDict


class AmqpMessage(BaseModel):
    """
    class AmqpMessage

    Represents an AMQP message with its associated channel, method, properties, and body.
    """

    channel: BlockingChannel
    method: Method
    properties: BasicProperties
    body: bytes

    model_config: ConfigDict = ConfigDict(arbitrary_types_allowed=True)
