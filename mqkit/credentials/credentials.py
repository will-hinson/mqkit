from abc import ABCMeta

from pydantic import BaseModel


class Credentials(BaseModel, metaclass=ABCMeta):
    def __repr__(self: "Credentials") -> str:
        return f"{self.__class__.__name__}(...)"

    def __str__(self: "Credentials") -> str:
        return self.__repr__()
