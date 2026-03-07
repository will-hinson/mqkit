from typing import List
from pydantic import BaseModel


class ExceptionHistoryEntry(BaseModel):
    exception_type: str
    exception_module: str
    exception_message: str
    traceback: List[str]
    retry_count: int
