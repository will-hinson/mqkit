from abc import ABCMeta, abstractmethod
from typing import Type

from pydantic import BaseModel
from yarl import URL

from ..connections import Connection


class Engine(BaseModel, metaclass=ABCMeta):
    @abstractmethod
    def connect(self: "Engine", queue: str) -> Connection:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_url(cls: Type["Engine"], url: URL) -> "Engine":
        raise NotImplementedError()
