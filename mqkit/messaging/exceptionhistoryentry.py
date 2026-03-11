"""
module mqkit.messaging.exceptionhistoryentry

This module defines the ExceptionHistoryEntry class, which is used to represent an entry in the
exception history of a failed message. The ExceptionHistoryEntry includes the type, module,
message, traceback, and retry count of the exception.
"""

from typing import List
from pydantic import BaseModel


class ExceptionHistoryEntry(BaseModel):
    """
    class ExceptionHistoryEntry

    This class represents an entry in the exception history of a failed message. The
    ExceptionHistoryEntry includes the type, module, message, traceback, and retry count of the
    exception.
    """

    exception_type: str
    exception_module: str
    exception_message: str
    traceback: List[str]
    retry_count: int
    origin_queue: str
