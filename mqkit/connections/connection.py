"""
module mqkit.connections.connection

Defines the abstract base class Connection for message queue connections.
"""

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Optional, List, Type

if TYPE_CHECKING:
    from ..declarations import Declaration
    from ..messaging import Forward, QueueMessage


class Connection(metaclass=ABCMeta):
    """
    class Connection

    Abstract base class for message queue connections.
    """

    @abstractmethod
    def acknowledge_failure(self: "Connection", message: "QueueMessage") -> None:
        """
        Acknowledge the failure of processing a message.

        Args:
            message (QueueMessage): The message to acknowledge.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def acknowledge_success(self: "Connection", message: QueueMessage) -> None:
        """
        Acknowledge the processing of a message.

        Args:
            message (QueueMessage): The message to acknowledge.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def declare_resources(self: "Connection", resources: List["Declaration"]) -> None:
        """
        Declare a messaging resource (e.g., queue, exchange).

        Args:
            resource (Declaration): The resource to declare.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def __enter__(self: "Connection") -> "Connection":
        """
        Enter the runtime context related to this object.

        Args:
            None

        Returns:
            Connection: The connection object itself.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def __exit__(
        self: "Connection", exc_type: Type[Exception], exc_value: Exception, traceback
    ) -> None:
        """
        Exit the runtime context related to this object.

        Args:
            exc_type: The exception type.
            exc_value: The exception value.
            traceback: The traceback object.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def forward_message(self: "Connection", forward: "Forward") -> None:
        """
        Forward a message to another queue.

        Args:
            forward (Forward): The forward object containing the target and message.

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def get_message(self: "Connection") -> "QueueMessage":
        """
        Block and wait for a message from the connected queue.

        Args:
            None

        Returns:
            bytes: The message received from the queue.

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()

    @abstractmethod
    def unblock(self: "Connection", message: Optional[str] = None) -> None:
        """
        Unblock the connection if it is blocked waiting for a message by
        submitting a sentinel message.

        Args:
            None

        Returns:
            None

        Raises:
            NotImplementedError: If the method is not implemented.
        """

        raise NotImplementedError()
