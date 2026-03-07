"""
module mqkit.messaging.retry.retrycontext

This module defines the RetryContext class, which is used to encapsulate the context of a
failed message when handling retries. The RetryContext includes the connection, the
message, and the exception that caused the failure.
"""

from typing import ClassVar
from pydantic import BaseModel, ConfigDict

from ...connections import Connection
from ..queuemessage import QueueMessage


class RetryContext(BaseModel):
    """
    class RetryContext

    This class encapsulates the context of a failed message when handling retries. The
    RetryContext includes the connection, the message, and the exception that caused the failure.
    """

    connection: Connection
    message: QueueMessage
    exception: Exception

    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
    )
