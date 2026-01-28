"""
module mqkit.declarations

Defines declaration classes for message queue applications.
"""

__all__ = [
    "Declaration",
    "ExchangeBinding",
    "ExchangeDeclaration",
]

from .exchangebinding import ExchangeBinding
from .exchangedeclaration import ExchangeDeclaration

Declaration = ExchangeDeclaration
