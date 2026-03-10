"""
module mqkit

A message queue toolkit for building and managing message queue applications.
"""

__all__ = [
    "App",
    "consume",
    "create_engine",
    "Exchange",
    "ImmediateRetryStrategy",
    "NoRetry",
    "NoRetryStrategy",
    "Queue",
    "Response",
    "RetryStrategy",
    #
    "apps",
    "credentials",
    "declarations",
    "endpoints",
    "engines",
    "errors",
    "events",
    "logging",
    "marshal",
    "messaging",
    "warnings",
    "workers",
]

from . import apps
from . import credentials
from . import declarations
from . import endpoints
from . import engines
from . import errors
from . import events
from . import logging
from . import marshal
from . import messaging
from . import warnings
from . import workers

from .messaging.retry import ImmediateRetryStrategy, NoRetryStrategy, RetryStrategy

from . import consume as _consume

consume = _consume.consume
del _consume

App = apps.App
Attributes = messaging.Attributes
create_engine = engines.create_engine
Engine = engines.Engine
Exchange = messaging.Exchange
NoRetry = errors.NoRetry
Queue = messaging.Queue
Response = messaging.Response
