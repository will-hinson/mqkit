"""
module mqkit.messaging.response

Defines the Response class for handling message responses.
"""

from typing import Dict, Generic, Optional, TypeVar

from pydantic import BaseModel, Field, PrivateAttr

T = TypeVar("T", bound=BaseModel)


class Response(BaseModel, Generic[T]):
    """
    class Response

    Represents a response message with data and optional headers.
    """

    content: T
    headers: Dict[str, str] = Field(default_factory=dict)
    topic: Optional[str] = None

    _serialized_data: Optional[bytes] = PrivateAttr(default=None)

    def __init__(
        self: "Response",
        content: T,
        headers: Optional[Dict[str, str]] = None,
        topic: Optional[str] = None,
    ) -> None:
        super().__init__(
            content=content,
            headers=headers or {},
            topic=topic,
        )

    @property
    def data(self: "Response") -> bytes:
        """
        Property data

        Gets the serialized data of the response.

        Returns:
            bytes: The serialized data if set

        Raises:
            ValueError: If the serialized data is not set.
        """

        if self._serialized_data is None:
            raise ValueError(
                "Serialized data is not set (Response has not been serialized)"
            )

        return self._serialized_data

    @data.setter
    def data(self: "Response", value: Optional[bytes]) -> None:
        """
        Property setter data

        Sets the serialized data of the response.

        Args:
            value (bytes): The serialized data to set.
        """

        if not isinstance(value, bytes):
            raise TypeError(
                f"Serialized data must be of type bytes, not {type(value).__name__}"
            )

        self._serialized_data = value

    @property
    def has_data(self: "Response") -> bool:
        """
        Property has_data

        Indicates whether the serialized data is set.

        Returns:
            bool: True if the serialized data is set, False otherwise.
        """

        return self._serialized_data is not None
