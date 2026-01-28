"""
module mqkit.declarations.exchangedeclaration

Defines the ExchangeDeclaration model representing an exchange declaration.
"""

from typing import List, Union

from pydantic import BaseModel, Field

from ..messaging import Exchange, Queue
from .exchangebinding import ExchangeBinding


class ExchangeDeclaration(BaseModel):
    """
    class ExchangeDeclaration

    Model representing an exchange declaration.
    """

    exchange: Exchange
    bindings: List[ExchangeBinding] = Field(default_factory=list)

    def bind(
        self: "ExchangeDeclaration", resource: Union[Exchange, Queue], topic: str
    ) -> "ExchangeDeclaration":
        """
        Bind a queue to the exchange.

        Args:
            queue (Queue): The queue to bind.

        Returns:
            None
        """

        self.bindings.append(
            ExchangeBinding(
                bound_resource=resource,
                topic=topic,
            )
        )
        return self

    def bind_exchange(
        self: "ExchangeDeclaration",
        exchange: Union[Exchange, str],
        topic: str,
    ) -> "ExchangeDeclaration":
        """
        Bind an exchange to the exchange.

        Args:
            exchange (Exchange): The exchange to bind.

        Returns:
            None
        """

        if isinstance(exchange, str):
            exchange = Exchange(name=exchange)

        return self.bind(
            resource=exchange,
            topic=topic,
        )

    def bind_queue(
        self: "ExchangeDeclaration",
        queue: Union[Queue, str],
        topic: str,
    ) -> "ExchangeDeclaration":
        """
        Bind a queue to the exchange.

        Args:
            queue (Queue): The queue to bind.

        Returns:
            None
        """

        if isinstance(queue, str):
            queue = Queue(name=queue)

        return self.bind(
            resource=queue,
            topic=topic,
        )
