"""
module mqkit.credentials.credentials

Defines the Credentials abstract base class for representing credentials used
for authenticating with message queue services.
"""

from abc import ABCMeta

from pydantic import BaseModel


class Credentials(BaseModel, metaclass=ABCMeta):
    """
    class Credentials

    Abstract base class for representing credentials used for authenticating
    with message queue services.
    """

    def __repr__(self: "Credentials") -> str:
        return f"{self.__class__.__name__}(...)"

    def __str__(self: "Credentials") -> str:
        return self.__repr__()
