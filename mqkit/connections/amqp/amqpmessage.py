from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.amqp_object import Method
from pydantic import BaseModel


class AmqpMessage(BaseModel):
    channel: BlockingChannel
    method: Method
    properties: BasicProperties
    body: bytes

    class Config:
        arbitrary_types_allowed = True
