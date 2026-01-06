from typing import Any, Dict

from pydantic import BaseModel


class QueueMessage(BaseModel):
    data: bytes
    attributes: Dict[str, Any]
