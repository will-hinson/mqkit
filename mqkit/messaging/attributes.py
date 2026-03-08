"""
module mqkit.marshal.attributes

Defines the Attributes class representing message attributes in the
message queue system.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from .exceptionhistoryentry import ExceptionHistoryEntry


class Attributes(BaseModel):
    """
    class Attributes

    Represents the attributes of a message in the message queue system.
    """

    headers: Dict[str, str]
    platform: Optional[Dict[str, Any]] = None
    forwarded: bool
    origin_queue: Optional[str] = None
    topic: Union[str, None]
    retry_count: int = 0
    previous_retry_count: int = 0
    is_dead_letter: bool
    exception_history: List[ExceptionHistoryEntry] = Field(..., default_factory=list)
