"""
module mqkit.marshal.typelessserializer

Defines the TypelessSerializer class for serializing and deserializing typeless data.
"""

from typing import Any, Dict, Optional

from ..errors import SerializeError
from .serializer import Serializer


class TypelessSerializer(Serializer):
    """
    class TypelessSerializer

    A serializer that handles typeless data represented as dictionaries.
    """

    def deserialize(self: "TypelessSerializer", data: bytes) -> Dict[str, Any]:
        return self._codec.decode(data)

    def serialize(
        self: "TypelessSerializer", data: Optional[Dict[str, Any]]
    ) -> Optional[bytes]:
        if data is None:
            return None
        if not isinstance(data, dict):
            raise SerializeError(
                f"TypelessSerializer can only serialize dict types but got {type(data).__name__}"
            )

        return self._codec.encode(data)
