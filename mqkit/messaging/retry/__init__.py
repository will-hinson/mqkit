"""
module mqkit.messaging.retry

Contains the retry strategy classes for handling failed messages. A retry strategy is responsible
for handling failed messages and determining whether to retry them or not. Subclasses of
RetryStrategy can implement different retry policies. The NoRetryStrategy is a simple
implementation of a retry strategy that never retries a failed message, and is the default
behavior if a retry strategy is not specified for a message handler
"""

__all__ = [
    "NoRetryStrategy",
    "RetryContext",
    "RetryStrategy",
]

from .noretrystrategy import NoRetryStrategy
from .retrycontext import RetryContext
from .retrystrategy import RetryStrategy
