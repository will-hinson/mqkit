"""
module mqkit.messaging.exchange

Defines the Exchange class for message queue exchanges.
"""

from typing import Union
from pydantic import BaseModel

from .exchangetype import ExchangeType


class Exchange(BaseModel):
    """
    class Exchange

    Model representing a message queue exchange.
    """

    name: str
    type: ExchangeType
    persistent: bool = True
    auto_delete: bool = False

    def __init__(
        # pylint: disable=redefined-builtin
        self: "Exchange",
        type: Union[str, ExchangeType] = ExchangeType.FANOUT,
        **data,
    ) -> None:
        if isinstance(type, str) and not isinstance(type, ExchangeType):
            data["type"] = ExchangeType(type)
        else:
            data["type"] = type

        super().__init__(**data)
