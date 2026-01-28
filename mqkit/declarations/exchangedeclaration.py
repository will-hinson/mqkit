"""
module mqkit.declarations.exchangedeclaration

Defines the ExchangeDeclaration model representing an exchange declaration.
"""

from typing import List

from pydantic import BaseModel, Field

from ..messaging import Exchange, Queue


class ExchangeDeclaration(BaseModel):
    """
    class ExchangeDeclaration

    Model representing an exchange declaration.
    """

    exchange: Exchange
    bindings: List[Queue] = Field(default_factory=list)
