"""
module mqkit.engines.engine

Defines the abstract Engine class for message queue engines. Engines are responsible
for creating connections to message queues.
"""

from abc import ABCMeta, abstractmethod
from typing import List, Type

from pydantic import BaseModel
from yarl import URL

from ..connections import Connection
from ..declarations import Declaration


class Engine(BaseModel, metaclass=ABCMeta):
    """
    class Engine

    An abstract base class for message queue engines. Engines are responsible for
    creating connections to message queues.
    """

    @abstractmethod
    def connect(
        self: "Engine",
        queue: str,
        persistent: bool = True,
        auto_delete: bool = False,
    ) -> Connection:
        """
        Create a connection to the specified message queue. Returns a Connection object
        of the appropriate type for the engine (e.g., AmqpConnection for AMQP engines)

        Args:
            queue (str): The name of the message queue to connect to.

        Returns:
            Connection: A connection object for the specified message queue.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def declare_resources(self: "Engine", resources: List[Declaration]) -> None:
        """
        Declare the necessary resources (exchanges, queues, bindings) for the engine.

        Args:
            resources (ExchangeDeclaration): The resources to declare.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_url(cls: Type["Engine"], url: URL) -> "Engine":
        """
        Create an engine instance from a URL.

        Args:
            url (URL): The URL to create the engine from.

        Returns:
            Engine: An instance of the engine.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()
