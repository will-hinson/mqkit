"""
module mqkit.marshal.attributes

Defines the Attributes class representing message attributes in the
message queue system.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel


class Attributes(BaseModel):
    """
    class Attributes

    Represents the attributes of a message in the message queue system.
    """

    headers: Dict[str, str]
    platform: Optional[Dict[str, Any]] = None
    forwarded: bool
    origin_queue: Optional[str] = None
