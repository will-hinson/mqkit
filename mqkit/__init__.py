"""
module mqkit

A message queue toolkit for building and managing message queue applications.
"""

__all__ = [
    "App",
    "consume",
    "create_engine",
    "NoRetry",
    "Queue",
    #
    "apps",
    "credentials",
    "endpoints",
    "engines",
    "errors",
    "events",
    "marshal",
    "messaging",
    "workers",
]

from . import apps
from . import credentials
from . import endpoints
from . import engines
from . import errors
from . import events
from . import marshal
from . import messaging
from . import workers

from . import consume as _consume

consume = _consume.consume
del _consume

App = apps.App
create_engine = engines.create_engine
NoRetry = errors.NoRetry
Queue = messaging.Queue
