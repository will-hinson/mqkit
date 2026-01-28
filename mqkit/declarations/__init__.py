"""
module mqkit.declarations

Defines declaration classes for message queue applications.
"""

__all__ = [
    "Declaration",
    "ExchangeBinding",
    "ExchangeDeclaration",
    "QueueDeclaration",
]

from typing import Union

from .exchangebinding import ExchangeBinding
from .exchangedeclaration import ExchangeDeclaration
from .queuedeclaration import QueueDeclaration

Declaration = Union[ExchangeDeclaration, QueueDeclaration]
